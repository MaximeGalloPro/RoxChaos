# frozen_string_literal: true

require 'json'

module RoxChaosStartRuns
  extend self

  MARKER_BEGIN = 'ROXCHAOS_JSON_BEGIN'
  MARKER_END = 'ROXCHAOS_JSON_END'
  RUN_SEQUENCE_KEY = 'roxia:run_id_seq'

  def run
    input = read_input
    organization_name = organization_name!(input)
    workflow_names = workflow_names!(input)
    organization = find_exact_organization!(organization_name)
    previous_organization = Current.organisation
    reservations = []

    begin
      Current.organisation = organization
      workflows = workflow_names.map { |name| find_exact_workflow!(name) }
      raise 'Workflow names resolved to the same List' unless workflows.map(&:id).uniq.length == workflows.length

      ApplicationRecord.transaction do
        reservations = workflows.map do |list|
          async_process_task = find_async_process_task!(list, organization)
          workflow_run = WorkflowRun.reserve_scheduled!(
            list: list,
            async_process_task: async_process_task,
            scheduled_for: Time.current,
            run_id: Redis.current.incr(RUN_SEQUENCE_KEY)
          )
          raise "Could not reserve a scheduled WorkflowRun for #{list.name.inspect}" unless workflow_run

          async_process_task.start_execution!
          [list, async_process_task, workflow_run]
        end
      end

      results = reservations.map do |list, async_process_task, workflow_run|
        begin
          job = AsyncTaskJob.perform_later(async_process_task.id, workflow_run.id)
          unless job.successfully_enqueued?
            raise(job.enqueue_error || "AsyncTaskJob was not enqueued for WorkflowRun #{workflow_run.id}")
          end
        rescue StandardError => e
          workflow_run.fail!(e)
          async_process_task.finish_execution! if async_process_task.reload.running?
          raise
        end

        workflow_run.reload
        {
          'id' => workflow_run.id,
          'run_id' => workflow_run.run_id,
          'status' => workflow_run.status,
          'scheduled_for' => workflow_run.scheduled_for,
          'list' => { 'id' => list.id, 'name' => list.name },
          'async_process_task_id' => async_process_task.id,
          'active_job_id' => job.job_id,
          'provider_job_id' => job.provider_job_id
        }
      end

      write_json('organization' => organization.name, 'runs' => results)
    ensure
      Current.organisation = previous_organization
    end
  end

  private

  def read_input
    value = JSON.parse($stdin.read)
    raise 'STDIN JSON must be an object' unless value.is_a?(Hash)

    value
  rescue JSON::ParserError => e
    raise "STDIN is not valid JSON: #{e.message}"
  end

  def organization_name!(input)
    value = input['organization'] || input['organization_name'] || input['organisation'] || input['organisation_name']
    raise 'STDIN must contain a non-empty organization name' unless value.is_a?(String) && value.present?

    value
  end

  def workflow_names!(input)
    value = input['workflows']
    unless value.is_a?(Array) && value.any? && value.all? { |name| name.is_a?(String) && name.present? }
      raise 'STDIN workflows must be a non-empty array of list names'
    end
    raise 'STDIN workflow list names must be unique' unless value.uniq.length == value.length

    value
  end

  def find_exact_organization!(name)
    matches = Organisation.where(name: name).to_a
    raise "Expected exactly one Organisation named #{name.inspect}, found #{matches.length}" unless matches.one?

    matches.first
  end

  def find_exact_workflow!(name)
    matches = List.where(name: name).to_a
    raise "Expected exactly one List named #{name.inspect} in the organization, found #{matches.length}" unless matches.one?

    matches.first
  end

  def find_async_process_task!(list, organization)
    generic_tasks = list.tasks.where(task_type: 'AsyncProcessTask').to_a
    unless generic_tasks.one?
      raise "List #{list.name.inspect} must have exactly one AsyncProcessTask, found #{generic_tasks.length}"
    end

    generic_task = generic_tasks.first
    matches = AsyncProcessTask.unscoped.where(task_id: generic_task.id).to_a
    valid = matches.one? &&
            matches.first.id == generic_task.task_type_id &&
            matches.first.organisation_id == organization.id
    raise "List #{list.name.inspect} has an invalid bidirectional AsyncProcessTask link" unless valid

    matches.first
  end

  def write_json(payload)
    $stdout.write("#{MARKER_BEGIN}\n")
    $stdout.write(JSON.pretty_generate(payload))
    $stdout.write("\n#{MARKER_END}\n")
  end
end

begin
  RoxChaosStartRuns.run
rescue StandardError => e
  warn "RoxChaos start failed: #{e.message}"
  exit 1
end
