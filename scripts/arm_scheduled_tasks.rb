# frozen_string_literal: true

require 'json'

module RoxChaosArmScheduledTasks
  extend self

  MARKER_BEGIN = 'ROXCHAOS_JSON_BEGIN'
  MARKER_END = 'ROXCHAOS_JSON_END'
  RECURRING_TASK_KEY = 'scheduled_tasks_scan'
  SCHEDULE_ATTRIBUTES = %w[
    active running last_run_at next_run_at recurrence_type recurrence_days scheduled_time
  ].freeze

  def run
    input = read_input
    organization = find_exact_organization!(organization_name!(input))
    workflow_names = workflow_names!(input)
    previous_organization = Current.organisation

    begin
      Current.organisation = organization
      lists = workflow_names.map { |name| find_exact_list!(name, organization) }
      raise 'Workflow names resolved to the same List' unless lists.map(&:id).uniq.length == 2

      async_process_tasks = lists.map { |list| find_async_process_task!(list, organization) }
      raise 'The dedicated database already contains WorkflowRun rows' if WorkflowRun.unscoped.exists?

      recurring_task = find_recurring_task!
      baseline_execution_id = SolidQueue::RecurringExecution
                              .includes(:job)
                              .where(task_key: RECURRING_TASK_KEY)
                              .filter_map { |execution| execution.id if execution.job&.finished_at.present? }
                              .max

      armed_at = nil
      target_scheduled_for = nil
      ApplicationRecord.transaction do
        raise 'The dedicated database already contains WorkflowRun rows' if WorkflowRun.unscoped.exists?

        armed_at = Time.zone.now
        target_scheduled_for = (armed_at + 1.minute).change(sec: 0)
        schedule_attributes = {
          active: true,
          running: false,
          last_run_at: nil,
          next_run_at: nil,
          recurrence_type: 'daily',
          recurrence_days: JSON.generate([]),
          scheduled_time: target_scheduled_for.strftime('%H:%M')
        }
        async_process_tasks.each do |async_process_task|
          async_process_task.lock!
          async_process_task.update!(schedule_attributes)
        end
      end

      write_json(
        'organization' => { 'id' => organization.id, 'name' => organization.name },
        'armed_at' => timestamp(armed_at),
        'target_scheduled_for' => timestamp(target_scheduled_for),
        'recurring' => {
          'baseline_execution_id' => baseline_execution_id,
          'task' => serialize_recurring_task(recurring_task)
        },
        'workflows' => lists.zip(async_process_tasks).map do |list, async_process_task|
          serialize_workflow(list, async_process_task.reload)
        end
      )
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
    unless value.is_a?(Array) && value.length == 2 && value.all? { |name| name.is_a?(String) && name.present? }
      raise 'STDIN workflows must contain exactly two non-empty List names'
    end
    raise 'STDIN workflow List names must be unique' unless value.uniq.length == 2

    value
  end

  def find_exact_organization!(name)
    matches = Organisation.unscoped.where(name: name).to_a
    raise "Expected exactly one Organisation named #{name.inspect}, found #{matches.length}" unless matches.one?

    matches.first
  end

  def find_exact_list!(name, organization)
    matches = List.unscoped.where(name: name, organisation_id: organization.id).to_a
    unless matches.one?
      raise "Expected exactly one List named #{name.inspect} in the organization, found #{matches.length}"
    end

    matches.first
  end

  def find_async_process_task!(list, organization)
    generic_tasks = Task.unscoped.where(list_id: list.id, task_type: 'AsyncProcessTask').to_a
    unless generic_tasks.one?
      raise "List #{list.name.inspect} must have exactly one AsyncProcessTask, found #{generic_tasks.length}"
    end

    generic_task = generic_tasks.first
    specific_tasks = AsyncProcessTask.unscoped.where(task_id: generic_task.id).to_a
    valid = generic_task.organisation_id == organization.id &&
            specific_tasks.one? &&
            specific_tasks.first.id == generic_task.task_type_id &&
            specific_tasks.first.organisation_id == organization.id
    raise "List #{list.name.inspect} has an invalid bidirectional AsyncProcessTask link" unless valid

    specific_tasks.first
  end

  def find_recurring_task!
    matches = SolidQueue::RecurringTask.where(key: RECURRING_TASK_KEY).to_a
    unless matches.one?
      raise "Expected exactly one SolidQueue::RecurringTask keyed #{RECURRING_TASK_KEY.inspect}, found #{matches.length}"
    end

    task = matches.first
    unless task.class_name == 'ScheduledTasksJob' && task.schedule == 'every minute'
      raise "Recurring task #{RECURRING_TASK_KEY.inspect} must run ScheduledTasksJob every minute"
    end

    task
  end

  def serialize_recurring_task(task)
    task.attributes.slice('id', 'key', 'class_name', 'schedule', 'queue_name', 'priority', 'static', 'description')
  end

  def serialize_workflow(list, async_process_task)
    {
      'workflow_id' => list.id,
      'workflow_name' => list.name,
      'list_id' => list.id,
      'async_process_task_id' => async_process_task.id,
      'list' => { 'id' => list.id, 'name' => list.name },
      'async_process_task' => { 'id' => async_process_task.id, 'task_id' => async_process_task.task_id }.merge(
        async_process_task.attributes.slice(*SCHEDULE_ATTRIBUTES)
      )
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
  RoxChaosArmScheduledTasks.run
rescue StandardError => e
  warn "RoxChaos schedule arming failed: #{e.message}"
  exit 1
end
