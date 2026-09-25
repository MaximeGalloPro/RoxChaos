# frozen_string_literal: true

require 'json'

module RoxChaosExportManifest
  extend self

  MARKER_BEGIN = 'ROXCHAOS_JSON_BEGIN'
  MARKER_END = 'ROXCHAOS_JSON_END'
  DEFAULT_WORKFLOWS = ['Flash Emplois', 'Flash Emplois - AUDIT'].freeze

  ORGANIZATION_ATTRIBUTES = %w[
    name slug url logo_url description location email phone contact_name
    contact_email contact_phone contact_position contact_language
  ].freeze
  THEME_ATTRIBUTES = %w[name description].freeze
  LIST_ATTRIBUTES = %w[name description].freeze
  TASK_ATTRIBUTES = %w[
    name description task_type step_order input_key output_key
    fine_tune_version_label fine_tune_version
  ].freeze
  TAG_ATTRIBUTES = %w[name description color].freeze
  RULE_ATTRIBUTES = %w[label description tag_on_conformity version].freeze

  SPECIFIC_ATTRIBUTES = {
    'ReformatTextTask' => %w[reformatting_instructions],
    'PointExtractorTask' => %w[number_of_points extraction_instructions],
    'TextScorerTask' => %w[scoring_prompt min_score max_score complementary_instructions],
    'DataSaverTask' => %w[label_source display_target],
    'AsyncProcessTask' => %w[callback_url scheduled_time recurrence_type recurrence_days active]
  }.freeze
  CONDITIONAL_ATTRIBUTES = %w[
    condition_parser_type data_source_type success_action_type
    success_notification_message success_comment success_status
    success_max_recursions failure_action_type failure_notification_message
    failure_comment failure_status failure_max_recursions execution_schedule
    retry_on_failure max_retries
  ].freeze
  SOURCE_ATTRIBUTES = {
    'LocalSource' => %w[
      local_path element_number element_selector data_type is_composite_key
      separator line_separator
    ],
    'ApiSource' => %w[url auth_token api_path query_param page per_page],
    'SftpSource' => %w[host port username password remote_path]
  }.freeze
  PARSER_ATTRIBUTES = {
    'DateParser' => %w[date_format days_offset check_type],
    'TextParser' => %w[case_sensitive pattern name description]
  }.freeze

  def run
    organization_name = ENV.fetch('ROXCHAOS_ORGANISATION', 'CD06')
    workflow_names = parse_workflow_names
    organization = find_exact_organization!(organization_name)
    previous_organization = Current.organisation

    begin
      Current.organisation = organization
      workflows = find_exact_workflows!(workflow_names)
      manifest = build_manifest(organization, workflows)
      write_json(manifest)
    ensure
      Current.organisation = previous_organization
    end
  end

  private

  def parse_workflow_names
    value = JSON.parse(ENV.fetch('ROXCHAOS_WORKFLOWS', JSON.generate(DEFAULT_WORKFLOWS)))
    unless value.is_a?(Array) && value.length == 2 && value.all? { |name| name.is_a?(String) && !name.empty? }
      raise 'ROXCHAOS_WORKFLOWS must be a JSON array containing exactly two non-empty list names'
    end
    raise 'ROXCHAOS_WORKFLOWS list names must be unique' unless value.uniq.length == value.length

    value
  rescue JSON::ParserError => e
    raise "ROXCHAOS_WORKFLOWS is not valid JSON: #{e.message}"
  end

  def find_exact_organization!(name)
    matches = Organisation.where(name: name).to_a
    raise "Expected exactly one Organisation named #{name.inspect}, found #{matches.length}" unless matches.one?

    matches.first
  end

  def find_exact_workflows!(names)
    workflows = names.map do |name|
      matches = List.where(name: name).to_a
      raise "Expected exactly one List named #{name.inspect} in the organization, found #{matches.length}" unless matches.one?

      matches.first
    end
    raise 'The requested workflow names resolved to the same List' unless workflows.map(&:id).uniq.length == 2

    workflows
  end

  def build_manifest(organization, lists)
    themes = lists.map(&:theme)
    assert_same_organization!(themes, organization, 'Theme')

    theme_keys = keyed_records(themes.uniq(&:id)) { |theme| theme_key(theme) }
    list_keys = keyed_records(lists) { |list| list_key(list) }
    tasks = lists.flat_map { |list| list.tasks.order(:step_order, :name).to_a }
    task_keys = keyed_records(tasks) { |task| task_key(task, list_keys.fetch(task.list_id)) }
    specifics = tasks.to_h { |task| [task.id, find_specific_task!(task, organization)] }
    validate_async_tasks!(lists, tasks)

    rules = selected_rules(specifics.values, organization)
    rule_keys = keyed_records(rules) { |rule| rule_key(rule) }
    tags = selected_tags(rules, specifics.values, organization)
    tag_keys = keyed_records(tags) { |tag| tag_key(tag) }

    workflow_payloads = lists.map do |list|
      list_tasks = tasks.select { |task| task.list_id == list.id }
      {
        'key' => "workflow:#{list.name}",
        'theme_key' => theme_keys.fetch(list.theme_id),
        'list' => {
          'key' => list_keys.fetch(list.id),
          'attributes' => attributes_for(list, LIST_ATTRIBUTES)
        },
        'tasks' => list_tasks.map do |task|
          export_task(
            task,
            specifics.fetch(task.id),
            task_keys: task_keys,
            list_keys: list_keys,
            rule_keys: rule_keys,
            tag_keys: tag_keys,
            organization: organization
          )
        end
      }
    end

    payload = {
      'schema_version' => 1,
      'organization' => {
        'key' => "organization:#{organization.name}",
        'attributes' => attributes_for(organization, ORGANIZATION_ATTRIBUTES)
      },
      'tags' => tags.sort_by { |tag| tag_keys.fetch(tag.id) }.map do |tag|
        { 'key' => tag_keys.fetch(tag.id), 'attributes' => attributes_for(tag, TAG_ATTRIBUTES) }
      end,
      'rules' => rules.sort_by { |rule| rule_keys.fetch(rule.id) }.map do |rule|
        export_rule(rule, rule_keys.fetch(rule.id), tag_keys.fetch(rule.tag_id.to_i))
      end,
      'themes' => themes.uniq(&:id).sort_by { |theme| theme_keys.fetch(theme.id) }.map do |theme|
        { 'key' => theme_keys.fetch(theme.id), 'attributes' => attributes_for(theme, THEME_ATTRIBUTES) }
      end,
      'workflows' => workflow_payloads
    }
    payload['expected_counts'] = expected_counts(payload)
    payload
  end

  def keyed_records(records)
    pairs = records.map { |record| [record.id, yield(record)] }
    duplicate_keys = pairs.group_by(&:last).select { |_key, values| values.length > 1 }.keys
    raise "Logical keys are ambiguous: #{duplicate_keys.join(', ')}" if duplicate_keys.any?

    pairs.to_h
  end

  def theme_key(theme)
    "theme:#{theme.name}"
  end

  def list_key(list)
    "list:#{list.theme.name}/#{list.name}"
  end

  def task_key(task, parent_list_key)
    "task:#{parent_list_key}/#{task.step_order}:#{task.name}"
  end

  def tag_key(tag)
    "tag:#{tag.name}"
  end

  def rule_key(rule)
    "rule:#{rule.label}:v#{rule.version}"
  end

  def find_specific_task!(task, organization)
    klass = task.task_type.safe_constantize
    unless klass && Task.task_types.key?(task.task_type) && klass < ApplicationRecord
      raise "Unsupported task type #{task.task_type.inspect} for Task #{task.name.inspect}"
    end

    linked = klass.unscoped.where(task_id: task.id).to_a
    unless linked.one? && linked.first.id == task.task_type_id
      raise "Task #{task.name.inspect} does not have one correct bidirectional #{task.task_type} link"
    end
    assert_same_organization!(linked, organization, task.task_type)
    linked.first
  end

  def validate_async_tasks!(lists, tasks)
    lists.each do |list|
      count = tasks.count { |task| task.list_id == list.id && task.task_type == 'AsyncProcessTask' }
      raise "List #{list.name.inspect} must have exactly one AsyncProcessTask, found #{count}" unless count == 1
    end
  end

  def selected_rules(specifics, organization)
    rule_ids = specifics.grep(RuleTask).map(&:rule_id).uniq
    rules = Rule.unscoped.where(id: rule_ids).to_a
    raise 'One or more RuleTask records reference a missing Rule' unless rules.length == rule_ids.length

    assert_same_organization!(rules, organization, 'Rule')
    rules
  end

  def selected_tags(rules, specifics, organization)
    conditional_tag_ids = specifics.grep(ConditionalTask).flat_map do |task|
      [task.success_tag_id, task.failure_tag_id]
    end
    tag_ids = (rules.map(&:tag_id) + conditional_tag_ids).compact.uniq
    tags = Tag.unscoped.where(id: tag_ids).to_a
    raise 'One or more selected records reference a missing Tag' unless tags.length == tag_ids.length

    assert_same_organization!(tags, organization, 'Tag')
    tags
  end

  def export_rule(rule, key, tag_key_value)
    {
      'key' => key,
      'tag_key' => tag_key_value,
      'attributes' => attributes_for(rule, RULE_ATTRIBUTES),
      'conformity_examples' => rule.rule_conformity_examples.order(:content).map do |example|
        { 'content' => example.content }
      end,
      'non_conformity_examples' => rule.rule_non_conformity_examples.order(:content).map do |example|
        { 'content' => example.content }
      end
    }
  end

  def export_task(task, specific, task_keys:, list_keys:, rule_keys:, tag_keys:, organization:)
    {
      'key' => task_keys.fetch(task.id),
      'attributes' => attributes_for(task, TASK_ATTRIBUTES),
      'specific_task' => export_specific_task(
        task,
        specific,
        task_keys: task_keys,
        list_keys: list_keys,
        rule_keys: rule_keys,
        tag_keys: tag_keys,
        organization: organization
      )
    }
  end

  def export_specific_task(task, specific, task_keys:, list_keys:, rule_keys:, tag_keys:, organization:)
    payload = { 'type' => task.task_type, 'attributes' => {} }
    case task.task_type
    when 'RetrieverTask'
      unless specific.data_source_type == 'LocalSource'
        raise "RoxChaos only supports LocalSource retrievers, got #{specific.data_source_type.inspect}"
      end
      payload['attributes'] = attributes_for(specific, %w[data_source_type include_key_in_result])
      source_payload = export_source!(specific.data_source_type, specific.data_source_id, organization)
      payload['local_source'] = source_payload
    when 'ConditionalTask'
      payload['attributes'] = attributes_for(specific, CONDITIONAL_ATTRIBUTES)
      payload['success_trigger_list_key'] = referenced_key(specific.success_trigger_list_id, list_keys, 'success trigger List')
      payload['failure_trigger_list_key'] = referenced_key(specific.failure_trigger_list_id, list_keys, 'failure trigger List')
      payload['success_tag_key'] = referenced_key(specific.success_tag_id, tag_keys, 'success Tag')
      payload['failure_tag_key'] = referenced_key(specific.failure_tag_id, tag_keys, 'failure Tag')
      payload['condition_parser'] = export_parser!(specific, organization) if specific.condition_parser_id.present?
      if specific.data_source_id.present?
        payload['data_source'] = export_source!(specific.data_source_type, specific.data_source_id, organization)
      end
    when 'DataSaverTask'
      payload['attributes'] = attributes_for(specific, SPECIFIC_ATTRIBUTES.fetch(task.task_type))
      payload['step_task_key'] = referenced_task_key(specific.step_task_id, task_keys)
    when 'RuleTask'
      payload['rule_key'] = rule_keys.fetch(specific.rule_id) do
        raise "RuleTask for #{task.name.inspect} references an unexported Rule"
      end
    else
      payload['attributes'] = attributes_for(specific, SPECIFIC_ATTRIBUTES.fetch(task.task_type))
    end
    payload
  end

  def export_source!(type, id, organization)
    raise "RoxChaos only supports LocalSource data sources, got #{type.inspect}" unless type == 'LocalSource'

    attributes = SOURCE_ATTRIBUTES[type]
    raise "Unsupported data source type #{type.inspect}" unless attributes
    raise "Missing #{type} reference" if id.blank?

    source = type.constantize.unscoped.find_by(id: id)
    raise "Missing #{type} source" unless source

    assert_same_organization!([source], organization, type)
    { 'type' => type, 'attributes' => attributes_for(source, attributes) }
  end

  def export_parser!(conditional_task, organization)
    type = conditional_task.condition_parser_type
    attributes = PARSER_ATTRIBUTES[type]
    raise "Unsupported condition parser type #{type.inspect}" unless attributes

    parser = type.constantize.unscoped.find_by(id: conditional_task.condition_parser_id)
    raise "Missing #{type} parser" unless parser

    assert_same_organization!([parser], organization, type)
    { 'type' => type, 'attributes' => attributes_for(parser, attributes) }
  end

  def referenced_key(id, key_map, label)
    return if id.blank?

    key_map.fetch(id) { raise "ConditionalTask references an unexported #{label}" }
  end

  def referenced_task_key(raw_id, task_keys)
    return if raw_id.blank?

    id = Integer(raw_id, exception: false)
    raise "DataSaverTask step_task_id #{raw_id.inspect} is not a Task ID" unless id

    task_keys.fetch(id) { raise 'DataSaverTask references a Task outside the selected workflows' }
  end

  def attributes_for(record, names)
    record.attributes.slice(*names)
  end

  def assert_same_organization!(records, organization, label)
    invalid = records.compact.reject { |record| record.organisation_id == organization.id }
    raise "#{label} record belongs to another organization" if invalid.any?
  end

  def expected_counts(payload)
    tasks = payload.fetch('workflows').flat_map { |workflow| workflow.fetch('tasks') }
    specifics = tasks.map { |task| task.fetch('specific_task') }
    rules = payload.fetch('rules')
    sources = specifics.flat_map { |specific| [specific['local_source'], specific['data_source']] }.compact
    parsers = specifics.filter_map { |specific| specific['condition_parser'] }

    {
      'organizations' => 1,
      'themes' => payload.fetch('themes').length,
      'workflows' => payload.fetch('workflows').length,
      'tasks' => tasks.length,
      'specific_tasks' => specifics.length,
      'tags' => payload.fetch('tags').length,
      'rules' => rules.length,
      'rule_conformity_examples' => rules.sum { |rule| rule.fetch('conformity_examples').length },
      'rule_non_conformity_examples' => rules.sum { |rule| rule.fetch('non_conformity_examples').length },
      'local_sources' => sources.count { |source| source.fetch('type') == 'LocalSource' },
      'api_sources' => sources.count { |source| source.fetch('type') == 'ApiSource' },
      'sftp_sources' => sources.count { |source| source.fetch('type') == 'SftpSource' },
      'date_parsers' => parsers.count { |parser| parser.fetch('type') == 'DateParser' },
      'text_parsers' => parsers.count { |parser| parser.fetch('type') == 'TextParser' }
    }
  end

  def write_json(payload)
    $stdout.write("#{MARKER_BEGIN}\n")
    $stdout.write(JSON.pretty_generate(payload))
    $stdout.write("\n#{MARKER_END}\n")
  end
end

begin
  RoxChaosExportManifest.run
rescue StandardError => e
  warn "RoxChaos export failed: #{e.message}"
  exit 1
end
