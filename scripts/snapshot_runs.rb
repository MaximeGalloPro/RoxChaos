# frozen_string_literal: true

require 'json'

module RoxChaosSnapshotRuns
  extend self

  MARKER_BEGIN = 'ROXCHAOS_JSON_BEGIN'
  MARKER_END = 'ROXCHAOS_JSON_END'

  def run
    run_ids = read_run_ids
    runs_by_id = WorkflowRun.unscoped.where(id: run_ids).index_by(&:id)
    missing = run_ids - runs_by_id.keys
    raise "WorkflowRun database IDs not found: #{missing.join(', ')}" if missing.any?

    previous_organization = Current.organisation
    snapshots = run_ids.map do |id|
      workflow_run = runs_by_id.fetch(id)
      organization = Organisation.find_by(id: workflow_run.organisation_id)
      raise "Organisation #{workflow_run.organisation_id} for WorkflowRun #{id} was not found" unless organization

      begin
        Current.organisation = organization
        snapshot_run(workflow_run.reload, organization)
      ensure
        Current.organisation = previous_organization
      end
    end

    write_json('runs' => snapshots)
  ensure
    Current.organisation = previous_organization if defined?(previous_organization)
  end

  private

  def read_run_ids
    input = JSON.parse($stdin.read)
    raise 'STDIN JSON must be an object' unless input.is_a?(Hash)

    ids = input['run_ids']
    unless ids.is_a?(Array) && ids.any? && ids.all? { |id| id.is_a?(Integer) && id.positive? }
      raise 'STDIN run_ids must be a non-empty array of positive WorkflowRun database IDs'
    end
    raise 'STDIN run_ids must be unique' unless ids.uniq.length == ids.length

    ids
  rescue JSON::ParserError => e
    raise "STDIN is not valid JSON: #{e.message}"
  end

  def snapshot_run(workflow_run, organization)
    list = workflow_run.list
    items = WorkflowRunItem.where(workflow_run_id: workflow_run.id).includes(:task).order(:id).to_a
    external_jobs = ExternalJob.where(workflow_run_item_id: items.map(&:id)).order(:id).to_a
    task_ids = list ? list.tasks.pluck(:id) : items.map(&:task_id).uniq
    task_logs = TaskLog.where(run_id: workflow_run.run_id, task_id: task_ids).includes(:task).order(:id).to_a
    analyses = Analysis.where(run_id: workflow_run.run_id, task_list_id: workflow_run.list_id).order(:id).to_a
    reference_tags = ReferenceTag.where(reference: analyses.map(&:reference), task_id: task_ids).order(:id).to_a
    document_logs = WorkflowDocumentLog.where(
      run_id: workflow_run.run_id,
      list_id: workflow_run.list_id
    ).order(:id).to_a

    {
      'id' => workflow_run.id,
      'run_id' => workflow_run.run_id,
      'organization' => { 'id' => organization.id, 'name' => organization.name },
      'list' => list && { 'id' => list.id, 'name' => list.name },
      'theme' => workflow_run.theme && { 'id' => workflow_run.theme.id, 'name' => workflow_run.theme.name },
      'async_process_task_id' => workflow_run.async_process_task_id,
      'trigger_type' => workflow_run.trigger_type,
      'status' => workflow_run.status,
      'expected_items_count' => workflow_run.expected_items_count,
      'processed_items_count' => workflow_run.processed_items_count,
      'retry_count' => workflow_run.retry_count,
      'duplicate_attempts_count' => workflow_run.duplicate_attempts_count,
      'started_at' => workflow_run.started_at,
      'last_activity_at' => workflow_run.last_activity_at,
      'completed_at' => workflow_run.completed_at,
      'failed_at' => workflow_run.failed_at,
      'scheduled_for' => workflow_run.scheduled_for,
      'last_duplicate_at' => workflow_run.last_duplicate_at,
      'last_detected_file_path' => workflow_run.last_detected_file_path,
      'last_detected_file_at' => workflow_run.last_detected_file_at,
      'last_processed_file_path' => workflow_run.last_processed_file_path,
      'last_processed_file_at' => workflow_run.last_processed_file_at,
      'message' => workflow_run.message,
      'counts' => snapshot_counts(items, external_jobs, task_logs, analyses, reference_tags, document_logs),
      'workflow_run_items' => items.map { |item| serialize_workflow_item(item, external_jobs) },
      'external_jobs' => external_jobs.map { |job| serialize_external_job(job) },
      'task_logs' => task_logs.map { |log| serialize_task_log(log) },
      'analyses' => analyses.map { |analysis| serialize_analysis(analysis) },
      'reference_tags' => reference_tags.map { |tag| serialize_reference_tag(tag) },
      'workflow_document_logs' => document_logs.map { |log| serialize_document_log(log) },
      'duplicate_workflow_run_item_groups' => duplicate_workflow_item_groups(items),
      'duplicate_success_task_log_groups' => duplicate_success_task_log_groups(task_logs),
      'duplicate_reference_tag_groups' => duplicate_reference_tag_groups(reference_tags),
      'duplicate_workflow_document_log_groups' => duplicate_document_log_groups(document_logs),
      'durable_side_effects' => {
        'analysis_ids' => analyses.map(&:id),
        'reference_tag_ids' => reference_tags.map(&:id),
        'external_job_ids' => external_jobs.map(&:id),
        'workflow_document_log_ids' => document_logs.map(&:id),
        'successful_task_log_ids' => task_logs.select { |log| log.state == 'SUCCESS' }.map(&:id)
      }
    }
  end

  def snapshot_counts(items, external_jobs, task_logs, analyses, reference_tags, document_logs)
    {
      'workflow_run_items' => items.length,
      'workflow_run_items_by_status' => frequency(items, &:status),
      'external_jobs' => external_jobs.length,
      'external_jobs_by_status' => frequency(external_jobs, &:status),
      'task_logs' => task_logs.length,
      'task_logs_by_state' => frequency(task_logs, &:state),
      'analyses' => analyses.length,
      'reference_tags' => reference_tags.length,
      'workflow_document_logs' => document_logs.length
    }
  end

  def frequency(records)
    records.group_by { |record| yield(record) }.transform_values(&:length).sort.to_h
  end

  def serialize_workflow_item(item, external_jobs)
    external_job = external_jobs.find { |job| job.workflow_run_item_id == item.id }
    item.attributes.slice(
      'id', 'workflow_run_id', 'task_id', 'document_reference', 'status',
      'idempotency_key', 'created_at', 'updated_at'
    ).merge(
      'task' => {
        'id' => item.task_id,
        'name' => item.task&.name,
        'task_type' => item.task&.task_type,
        'step_order' => item.task&.step_order
      },
      'external_job_id' => external_job&.id
    )
  end

  def serialize_external_job(job)
    job.attributes.slice(
      'id', 'workflow_run_item_id', 'organisation_id', 'job_id',
      'idempotency_key', 'name', 'source', 'status',
      'last_error', 'next_retry_at', 'next_check_at', 'lease_owner',
      'lease_until', 'attempt_token', 'created_at', 'updated_at'
    )
  end

  def serialize_task_log(log)
    log.attributes.slice(
      'id', 'task_id', 'reference', 'state', 'run_id',
      'execution_time_ms', 'created_at', 'updated_at'
    ).merge(
      'task' => {
        'id' => log.task_id,
        'name' => log.task&.name,
        'task_type' => log.task&.task_type,
        'step_order' => log.task&.step_order
      }
    )
  end

  def serialize_analysis(analysis)
    analysis.attributes.slice('id', 'run_id', 'task_list_id', 'reference', 'is_audit', 'created_at')
  end

  def serialize_reference_tag(tag)
    tag.attributes.slice('id', 'reference', 'tag_id', 'task_id', 'status', 'created_at')
  end

  def serialize_document_log(log)
    log.attributes.slice('id', 'run_id', 'list_id', 'file_name', 'file_path', 'status', 'created_at')
  end

  def duplicate_workflow_item_groups(items)
    items.group_by { |item| [item.workflow_run_id, item.task_id, item.document_reference] }
         .filter_map do |(workflow_run_id, task_id, document_reference), group|
      next unless group.length > 1

      {
        'workflow_run_id' => workflow_run_id,
        'task_id' => task_id,
        'document_reference' => document_reference,
        'count' => group.length,
        'ids' => group.map(&:id),
        'statuses' => group.map(&:status),
        'idempotency_keys' => group.map(&:idempotency_key)
      }
    end
  end

  def duplicate_success_task_log_groups(task_logs)
    task_logs.select { |log| log.state == 'SUCCESS' }
             .group_by { |log| [log.run_id, log.task_id, log.reference] }
             .filter_map do |(run_id, task_id, reference), group|
      next unless group.length > 1

      {
        'run_id' => run_id,
        'task_id' => task_id,
        'reference' => reference,
        'count' => group.length,
        'ids' => group.map(&:id)
      }
    end
  end

  def duplicate_reference_tag_groups(reference_tags)
    reference_tags.group_by { |tag| [tag.reference, tag.tag_id, tag.task_id, tag.status] }
                  .filter_map do |(reference, tag_id, task_id, status), group|
      next unless group.length > 1

      {
        'reference' => reference,
        'tag_id' => tag_id,
        'task_id' => task_id,
        'status' => status,
        'count' => group.length,
        'ids' => group.map(&:id)
      }
    end
  end

  def duplicate_document_log_groups(document_logs)
    document_logs.group_by { |log| [log.run_id, log.list_id, log.file_path] }
                 .filter_map do |(run_id, list_id, file_path), group|
      next unless group.length > 1

      {
        'run_id' => run_id,
        'list_id' => list_id,
        'file_path' => file_path,
        'count' => group.length,
        'ids' => group.map(&:id),
        'statuses' => group.map(&:status)
      }
    end
  end

  def write_json(payload)
    $stdout.write("#{MARKER_BEGIN}\n")
    $stdout.write(JSON.pretty_generate(payload))
    $stdout.write("\n#{MARKER_END}\n")
  end
end

begin
  RoxChaosSnapshotRuns.run
rescue StandardError => e
  warn "RoxChaos snapshot failed: #{e.message}"
  exit 1
end
