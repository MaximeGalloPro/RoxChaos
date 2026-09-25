# frozen_string_literal: true

require 'json'
require 'time'

module RoxChaosDiscoverScheduledRuns
  extend self

  MARKER_BEGIN = 'ROXCHAOS_JSON_BEGIN'
  MARKER_END = 'ROXCHAOS_JSON_END'
  RECURRING_TASK_KEY = 'scheduled_tasks_scan'

  def run
    context = normalize_input(read_input)
    run_matches = context.fetch('workflows').map { |workflow| find_run_matches(workflow, context.fetch('armed_at')) }
    assert_no_duplicate_runs!(context.fetch('workflows'), run_matches)

    runs = run_matches.map(&:first)
    if runs.any?(&:nil?)
      write_json(waiting_payload(context, runs, 'workflow_runs'))
      return
    end

    validate_runs!(context, runs)
    scanner = find_scanner(context, runs)
    unless scanner
      write_json(waiting_payload(context, runs, 'completed_scheduled_tasks_scan'))
      return
    end

    deliveries = matching_deliveries!(scanner.fetch('job'), context.fetch('workflows'), runs)
    write_json(
      base_payload(context).merge(
        'ready' => true,
        'runs' => serialize_runs(context.fetch('workflows'), runs),
        'scanner' => serialize_scanner(scanner),
        'deliveries' => deliveries,
        'scheduled_period_row_counts' => scheduled_period_row_counts(context)
      )
    )
  end

  private

  def read_input
    raw = $stdin.read
    if raw.include?(MARKER_BEGIN) || raw.include?(MARKER_END)
      match = raw.match(/#{Regexp.escape(MARKER_BEGIN)}\s*(.*?)\s*#{Regexp.escape(MARKER_END)}/m)
      raise 'STDIN contains incomplete RoxChaos JSON markers' unless match

      raw = match[1]
    end
    value = JSON.parse(raw)
    raise 'STDIN JSON must be an object' unless value.is_a?(Hash)

    value
  rescue JSON::ParserError => e
    raise "STDIN is not valid JSON: #{e.message}"
  end

  def normalize_input(input)
    arm_result = input['arm_result'] || input['arm'] || input
    raise 'Arm result must be a JSON object' unless arm_result.is_a?(Hash)

    recurring = arm_result['recurring']
    recurring = {} unless recurring.is_a?(Hash)
    recurring_task = recurring['task']
    recurring_task = {} unless recurring_task.is_a?(Hash)
    task_key = recurring_task['key'] || arm_result['recurring_task_key'] || RECURRING_TASK_KEY
    raise "Recurring task key must be #{RECURRING_TASK_KEY.inspect}" unless task_key == RECURRING_TASK_KEY

    baseline_id = if recurring.key?('baseline_execution_id')
                    recurring['baseline_execution_id']
                  else
                    arm_result['recurring_baseline_id'] || arm_result['baseline_recurring_execution_id']
                  end
    unless baseline_id.nil? || (baseline_id.is_a?(Integer) && baseline_id >= 0)
      raise 'Recurring baseline execution ID must be null or a non-negative integer'
    end

    workflows = arm_result['workflows']
    unless workflows.is_a?(Array) && workflows.length == 2
      raise 'Arm result workflows must contain exactly two entries'
    end

    normalized_workflows = workflows.map { |workflow| normalize_workflow(workflow) }
    unless normalized_workflows.map { |workflow| workflow.fetch('list_id') }.uniq.length == 2 &&
           normalized_workflows.map { |workflow| workflow.fetch('async_process_task_id') }.uniq.length == 2
      raise 'Armed List and AsyncProcessTask IDs must each be unique'
    end

    armed_at = parse_timestamp!(arm_result['armed_at'], 'armed_at')
    target_scheduled_for = parse_timestamp!(arm_result['target_scheduled_for'], 'target_scheduled_for')
    unless target_scheduled_for.sec.zero? && target_scheduled_for.usec.zero?
      raise 'target_scheduled_for must have zero seconds and sub-seconds'
    end

    {
      'organization' => arm_result['organization'],
      'armed_at' => armed_at,
      'target_scheduled_for' => target_scheduled_for,
      'baseline_execution_id' => baseline_id,
      'task_key' => task_key,
      'workflows' => normalized_workflows
    }
  end

  def normalize_workflow(workflow)
    raise 'Each armed workflow must be a JSON object' unless workflow.is_a?(Hash)

    list = workflow['list'].is_a?(Hash) ? workflow['list'] : {}
    async_process_task = workflow['async_process_task'].is_a?(Hash) ? workflow['async_process_task'] : {}
    list_id = list['id'] || workflow['list_id'] || workflow['workflow_id']
    async_process_task_id = async_process_task['id'] || workflow['async_process_task_id']
    unless list_id.is_a?(Integer) && list_id.positive?
      raise 'Each armed workflow must contain a positive List ID'
    end
    unless async_process_task_id.is_a?(Integer) && async_process_task_id.positive?
      raise 'Each armed workflow must contain a positive AsyncProcessTask ID'
    end

    {
      'list_id' => list_id,
      'list_name' => list['name'] || workflow['workflow_name'],
      'async_process_task_id' => async_process_task_id
    }
  end

  def parse_timestamp!(value, name)
    raise "#{name} must be an ISO 8601 string" unless value.is_a?(String)

    Time.iso8601(value).in_time_zone
  rescue ArgumentError
    raise "#{name} must be an ISO 8601 string"
  end

  def find_run_matches(workflow, armed_at)
    WorkflowRun.unscoped
               .where(
                 list_id: workflow.fetch('list_id'),
                 async_process_task_id: workflow.fetch('async_process_task_id')
               )
               .where('created_at > ?', armed_at)
               .order(:id)
               .to_a
  end

  def assert_no_duplicate_runs!(workflows, run_matches)
    workflows.zip(run_matches).each do |workflow, matches|
      next if matches.length <= 1

      raise "Expected one WorkflowRun for List ##{workflow.fetch('list_id')} and " \
            "AsyncProcessTask ##{workflow.fetch('async_process_task_id')}, found #{matches.length}"
    end
  end

  def validate_runs!(context, runs)
    context.fetch('workflows').zip(runs).each do |workflow, workflow_run|
      raise "WorkflowRun ##{workflow_run.id} is not scheduled" unless workflow_run.trigger_type == 'scheduled'
      unless workflow_run.scheduled_for == context.fetch('target_scheduled_for') && workflow_run.scheduled_for&.sec&.zero?
        raise "WorkflowRun ##{workflow_run.id} has an unexpected scheduled_for"
      end

      count = scheduled_period_count(workflow, context.fetch('target_scheduled_for'))
      raise "Expected one scheduled-period row for WorkflowRun ##{workflow_run.id}, found #{count}" unless count == 1
    end
  end

  def find_scanner(context, runs)
    baseline_id = context.fetch('baseline_execution_id') || 0
    candidates = SolidQueue::RecurringExecution
                 .includes(:job)
                 .where(task_key: context.fetch('task_key'))
                 .where('id > ?', baseline_id)
                 .order(:id)
                 .filter_map do |execution|
      job = execution.job
      next unless job&.class_name == 'ScheduledTasksJob' && job.finished_at.present?
      next unless runs.all? { |workflow_run| job.created_at <= workflow_run.created_at && workflow_run.created_at <= job.finished_at }

      { 'execution' => execution, 'job' => job }
    end

    raise "Multiple completed scanner jobs contain both WorkflowRun creations: #{candidates.length}" if candidates.length > 1

    candidates.first
  end

  def matching_deliveries!(scanner_job, workflows, runs)
    expected_pairs = workflows.zip(runs).map do |workflow, workflow_run|
      [workflow.fetch('async_process_task_id'), workflow_run.id]
    end
    deliveries = SolidQueue::Job
                 .where(class_name: 'AsyncTaskJob', created_at: scanner_job.created_at..scanner_job.finished_at)
                 .order(:id)
                 .map { |job| serialize_delivery(job) }
    matching = deliveries.select { |delivery| expected_pairs.include?(delivery.fetch('argument_pair')) }
    counts = matching.group_by { |delivery| delivery.fetch('argument_pair') }.transform_values(&:length)
    invalid_pairs = expected_pairs.reject { |pair| counts[pair] == 1 }
    unless invalid_pairs.empty? && matching.length == 2
      raise "Expected one AsyncTaskJob delivery for each scheduled run; invalid pairs: #{invalid_pairs.inspect}"
    end

    matching
  end

  def serialize_delivery(job)
    payload = job.arguments
    unless payload.is_a?(Hash) && payload['arguments'].is_a?(Array)
      raise "SolidQueue::Job ##{job.id} has an invalid Active Job argument payload"
    end

    arguments = ActiveJob::Arguments.deserialize(payload.fetch('arguments'))
    unless arguments.length == 2 && arguments.all? { |argument| argument.is_a?(Integer) && argument.positive? }
      raise "AsyncTaskJob SolidQueue::Job ##{job.id} must contain two positive integer arguments"
    end

    {
      'solid_queue_job_id' => job.id,
      'active_job_id' => job.active_job_id,
      'argument_pair' => arguments,
      'created_at' => timestamp(job.created_at),
      'scheduled_at' => timestamp(job.scheduled_at),
      'finished_at' => timestamp(job.finished_at)
    }
  end

  def serialize_runs(workflows, runs)
    workflows.zip(runs).filter_map do |workflow, workflow_run|
      next unless workflow_run

      list = List.unscoped.find_by(id: workflow.fetch('list_id'))
      {
        'id' => workflow_run.id,
        'run_id' => workflow_run.run_id,
        'list' => {
          'id' => workflow_run.list_id,
          'name' => list&.name || workflow['list_name']
        },
        'status' => workflow_run.status,
        'trigger_type' => workflow_run.trigger_type,
        'scheduled_for' => timestamp(workflow_run.scheduled_for),
        'created_at' => timestamp(workflow_run.created_at),
        'async_process_task_id' => workflow_run.async_process_task_id
      }
    end
  end

  def serialize_scanner(scanner)
    execution = scanner.fetch('execution')
    job = scanner.fetch('job')
    {
      'recurring_execution' => {
        'id' => execution.id,
        'task_key' => execution.task_key,
        'run_at' => timestamp(execution.run_at),
        'created_at' => timestamp(execution.created_at),
        'job_id' => execution.job_id
      },
      'job' => {
        'id' => job.id,
        'active_job_id' => job.active_job_id,
        'class_name' => job.class_name,
        'queue_name' => job.queue_name,
        'created_at' => timestamp(job.created_at),
        'scheduled_at' => timestamp(job.scheduled_at),
        'finished_at' => timestamp(job.finished_at)
      }
    }
  end

  def scheduled_period_row_counts(context)
    context.fetch('workflows').map do |workflow|
      {
        'list_id' => workflow.fetch('list_id'),
        'async_process_task_id' => workflow.fetch('async_process_task_id'),
        'scheduled_for' => timestamp(context.fetch('target_scheduled_for')),
        'count' => scheduled_period_count(workflow, context.fetch('target_scheduled_for'))
      }
    end
  end

  def scheduled_period_count(workflow, target_scheduled_for)
    WorkflowRun.unscoped.where(
      list_id: workflow.fetch('list_id'),
      async_process_task_id: workflow.fetch('async_process_task_id'),
      scheduled_for: target_scheduled_for
    ).count
  end

  def waiting_payload(context, runs, waiting_for)
    base_payload(context).merge(
      'ready' => false,
      'waiting_for' => waiting_for,
      'runs' => serialize_runs(context.fetch('workflows'), runs),
      'scheduled_period_row_counts' => scheduled_period_row_counts(context)
    )
  end

  def base_payload(context)
    {
      'organization' => context['organization'],
      'armed_at' => timestamp(context.fetch('armed_at')),
      'target_scheduled_for' => timestamp(context.fetch('target_scheduled_for')),
      'recurring_baseline_execution_id' => context.fetch('baseline_execution_id')
    }
  end

  def timestamp(value)
    value&.iso8601(6)
  end

  def write_json(payload)
    $stdout.write("#{MARKER_BEGIN}\n")
    $stdout.write(JSON.pretty_generate(payload))
    $stdout.write("\n#{MARKER_END}\n")
  end
end

begin
  RoxChaosDiscoverScheduledRuns.run
rescue StandardError => e
  warn "RoxChaos scheduled-run discovery failed: #{e.message}"
  exit 1
end
