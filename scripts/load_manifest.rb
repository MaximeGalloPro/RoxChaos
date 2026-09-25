# frozen_string_literal: true

require 'json'

module RoxChaosLoadManifest
  extend self

  MARKER_BEGIN = 'ROXCHAOS_JSON_BEGIN'
  MARKER_END = 'ROXCHAOS_JSON_END'

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
  EXPECTED_COUNT_KEYS = %w[
    organizations themes workflows tasks specific_tasks tags rules
    rule_conformity_examples rule_non_conformity_examples local_sources
    api_sources sftp_sources date_parsers text_parsers
  ].freeze

  def run
    manifest = read_manifest
    validate_manifest!(manifest)
    if Organisation.exists? && ENV.fetch('ROXCHAOS_ALLOW_NONEMPTY', '0') != '1'
      raise 'Organisation table is not empty; set ROXCHAOS_ALLOW_NONEMPTY=1 to override'
    end

    previous_organization = Current.organisation
    organization = nil
    actual_counts = nil

    begin
      ApplicationRecord.transaction do
        organization = create_configuration!(manifest)
        actual_counts = validate_loaded_configuration!(manifest, organization)
      end
    ensure
      Current.organisation = previous_organization
    end

    write_json(
      'schema_version' => 1,
      'organization' => organization.name,
      'workflows' => manifest.fetch('workflows').map { |workflow| workflow.fetch('list').fetch('attributes').fetch('name') },
      'schedules_disabled' => schedules_disabled?,
      'counts' => actual_counts
    )
  end

  private

  def read_manifest
    raw = $stdin.read
    if raw.include?(MARKER_BEGIN) || raw.include?(MARKER_END)
      match = raw.match(/#{Regexp.escape(MARKER_BEGIN)}\s*(.*?)\s*#{Regexp.escape(MARKER_END)}/m)
      raise 'STDIN contains incomplete RoxChaos JSON markers' unless match

      raw = match[1]
    end
    JSON.parse(raw)
  rescue JSON::ParserError => e
    raise "STDIN is not valid JSON: #{e.message}"
  end

  def validate_manifest!(manifest)
    raise 'Manifest root must be a JSON object' unless manifest.is_a?(Hash)
    raise "Unsupported schema_version #{manifest['schema_version'].inspect}" unless manifest['schema_version'] == 1

    workflows = array!(manifest, 'workflows')
    raise "Manifest must contain exactly two workflows, found #{workflows.length}" unless workflows.length == 2

    hash!(manifest, 'organization')
    array!(manifest, 'themes')
    array!(manifest, 'tags')
    array!(manifest, 'rules')
    index_by_key!(manifest.fetch('themes'), 'theme')
    index_by_key!(manifest.fetch('tags'), 'tag')
    index_by_key!(manifest.fetch('rules'), 'rule')
    index_by_key!(workflows, 'workflow')

    list_entries = workflows.map { |workflow| hash!(workflow, 'list') }
    index_by_key!(list_entries, 'list')
    task_entries = workflows.flat_map { |workflow| array!(workflow, 'tasks') }
    index_by_key!(task_entries, 'task')

    list_names = list_entries.map { |entry| hash!(entry, 'attributes')['name'] }
    raise 'Workflow list names must be non-empty and unique' unless list_names.all?(&:present?) && list_names.uniq.length == 2

    expected = hash!(manifest, 'expected_counts')
    unknown_count_keys = expected.keys - EXPECTED_COUNT_KEYS
    missing_count_keys = EXPECTED_COUNT_KEYS - expected.keys
    raise "Unknown expected_counts keys: #{unknown_count_keys.join(', ')}" if unknown_count_keys.any?
    raise "Missing expected_counts keys: #{missing_count_keys.join(', ')}" if missing_count_keys.any?
    raise 'All expected_counts values must be non-negative integers' unless expected.values.all? { |value| value.is_a?(Integer) && value >= 0 }

    computed = manifest_counts(manifest)
    raise "Manifest expected_counts mismatch: expected #{expected.inspect}, computed #{computed.inspect}" unless expected == computed
  end

  def create_configuration!(manifest)
    organization_data = hash!(manifest, 'organization')
    require_key!(organization_data, 'key', 'organization')
    organization = Organisation.create!(safe_attributes(organization_data, ORGANIZATION_ATTRIBUTES, 'organization'))
    Current.organisation = organization

    tags = create_tags!(manifest, organization)
    rules = create_rules!(manifest, organization, tags)
    themes = create_themes!(manifest, organization)
    lists = create_lists!(manifest, organization, themes)
    tasks, task_entries = create_generic_tasks!(manifest, organization, lists)
    create_specific_tasks!(task_entries, tasks, lists, tags, rules, organization)
    organization
  end

  def create_tags!(manifest, organization)
    index_by_key!(manifest.fetch('tags'), 'tag').transform_values do |entry|
      Tag.create!(safe_attributes(entry, TAG_ATTRIBUTES, 'tag').merge('organisation_id' => organization.id))
    end
  end

  def create_rules!(manifest, organization, tags)
    index_by_key!(manifest.fetch('rules'), 'rule').transform_values do |entry|
      tag = tags.fetch(require_key!(entry, 'tag_key', 'rule')) do
        raise "Rule references unknown tag key #{entry['tag_key'].inspect}"
      end
      rule = Rule.create!(
        safe_attributes(entry, RULE_ATTRIBUTES, 'rule').merge(
          'tag_id' => tag.id,
          'organisation_id' => organization.id
        )
      )
      array!(entry, 'conformity_examples').each do |example|
        attributes = exact_attributes!(example, %w[content], 'conformity example')
        rule.rule_conformity_examples.create!(attributes.merge('organisation_id' => organization.id))
      end
      array!(entry, 'non_conformity_examples').each do |example|
        attributes = exact_attributes!(example, %w[content], 'non-conformity example')
        rule.rule_non_conformity_examples.create!(attributes.merge('organisation_id' => organization.id))
      end
      rule
    end
  end

  def create_themes!(manifest, organization)
    index_by_key!(manifest.fetch('themes'), 'theme').transform_values do |entry|
      Theme.create!(safe_attributes(entry, THEME_ATTRIBUTES, 'theme').merge('organisation_id' => organization.id))
    end
  end

  def create_lists!(manifest, organization, themes)
    manifest.fetch('workflows').to_h do |workflow|
      theme_key = require_key!(workflow, 'theme_key', 'workflow')
      theme = themes.fetch(theme_key) { raise "Workflow references unknown theme key #{theme_key.inspect}" }
      list_entry = hash!(workflow, 'list')
      list_key = require_key!(list_entry, 'key', 'list')
      list = List.create!(
        safe_attributes(list_entry, LIST_ATTRIBUTES, 'list').merge(
          'theme_id' => theme.id,
          'organisation_id' => organization.id
        )
      )
      [list_key, list]
    end
  end

  def create_generic_tasks!(manifest, organization, lists)
    tasks = {}
    entries = []
    manifest.fetch('workflows').each do |workflow|
      list_key = workflow.fetch('list').fetch('key')
      list = lists.fetch(list_key)
      workflow.fetch('tasks').each do |entry|
        key = require_key!(entry, 'key', 'task')
        attributes = safe_attributes(entry, TASK_ATTRIBUTES, "task #{key}")
        task = Task.create!(attributes.merge('list_id' => list.id, 'organisation_id' => organization.id))
        tasks[key] = task
        entries << [entry, task]
      end
    end
    [tasks, entries]
  end

  def create_specific_tasks!(entries, tasks, lists, tags, rules, organization)
    entries.each do |entry, task|
      specific_data = hash!(entry, 'specific_task')
      type = require_key!(specific_data, 'type', "specific task #{entry['key']}")
      raise "Task type mismatch for #{entry['key']}" unless type == task.task_type

      specific = case type
                 when 'RetrieverTask'
                   create_retriever_task!(specific_data, task, organization)
                 when 'ConditionalTask'
                   create_conditional_task!(specific_data, task, lists, tags, organization)
                 when 'DataSaverTask'
                   create_data_saver_task!(specific_data, task, tasks, organization)
                 when 'RuleTask'
                   create_rule_task!(specific_data, task, rules, organization)
                 else
                   create_simple_specific_task!(specific_data, task, organization)
                 end
      task.update!(task_type_id: specific.id)
    end
  end

  def create_retriever_task!(data, task, organization)
    attributes = safe_specific_attributes(data, %w[data_source_type include_key_in_result], task)
    unless attributes['data_source_type'] == 'LocalSource'
      raise "RoxChaos only supports LocalSource retrievers, got #{attributes['data_source_type'].inspect}"
    end
    source_data = hash!(data, 'local_source')
    source = create_source!(source_data, attributes['data_source_type'], organization)
    RetrieverTask.create!(
      attributes.merge(
        'data_source_id' => source.id,
        'task_id' => task.id,
        'organisation_id' => organization.id
      )
    )
  end

  def create_conditional_task!(data, task, lists, tags, organization)
    attributes = safe_specific_attributes(data, CONDITIONAL_ATTRIBUTES, task)
    parser = optional_parser!(data['condition_parser'], attributes['condition_parser_type'], organization)
    source = optional_source!(data['data_source'], attributes['data_source_type'], organization)
    attributes.merge!(
      'condition_parser_id' => parser&.id,
      'data_source_id' => source&.id,
      'success_trigger_list_id' => optional_reference!(data['success_trigger_list_key'], lists, 'List')&.id,
      'failure_trigger_list_id' => optional_reference!(data['failure_trigger_list_key'], lists, 'List')&.id,
      'success_tag_id' => optional_reference!(data['success_tag_key'], tags, 'Tag')&.id,
      'failure_tag_id' => optional_reference!(data['failure_tag_key'], tags, 'Tag')&.id,
      'task_id' => task.id,
      'organisation_id' => organization.id
    )
    ConditionalTask.create!(attributes)
  end

  def create_data_saver_task!(data, task, tasks, organization)
    attributes = safe_specific_attributes(data, SPECIFIC_ATTRIBUTES.fetch('DataSaverTask'), task)
    step_task = optional_reference!(data['step_task_key'], tasks, 'Task')
    DataSaverTask.create!(
      attributes.merge(
        'step_task_id' => step_task&.id&.to_s,
        'task_id' => task.id,
        'organisation_id' => organization.id
      )
    )
  end

  def create_rule_task!(data, task, rules, organization)
    exact_attributes!(hash!(data, 'attributes'), [], "RuleTask attributes for #{task.name}")
    rule_key = require_key!(data, 'rule_key', "RuleTask #{task.name}")
    rule = rules.fetch(rule_key) { raise "RuleTask references unknown rule key #{rule_key.inspect}" }
    RuleTask.create!(task: task, rule: rule, organisation_id: organization.id)
  end

  def create_simple_specific_task!(data, task, organization)
    allowed = SPECIFIC_ATTRIBUTES[task.task_type]
    raise "Unsupported task type #{task.task_type.inspect}" unless allowed

    attributes = safe_specific_attributes(data, allowed, task)
    attributes['active'] = false if task.task_type == 'AsyncProcessTask' && schedules_disabled?
    task.task_type.constantize.create!(
      attributes.merge('task_id' => task.id, 'organisation_id' => organization.id)
    )
  end

  def create_source!(data, expected_type, organization)
    type = require_key!(data, 'type', 'data source')
    raise "Data source type mismatch: expected #{expected_type.inspect}, got #{type.inspect}" unless type == expected_type
    raise "RoxChaos only supports LocalSource data sources, got #{type.inspect}" unless type == 'LocalSource'

    allowed = SOURCE_ATTRIBUTES[type]
    raise "Unsupported data source type #{type.inspect}" unless allowed

    type.constantize.create!(safe_attributes(data, allowed, type).merge('organisation_id' => organization.id))
  end

  def optional_source!(data, expected_type, organization)
    if expected_type.present?
      raise "Missing embedded #{expected_type} data source" unless data.is_a?(Hash)

      create_source!(data, expected_type, organization)
    elsif data.present?
      raise 'Embedded data source is present without data_source_type'
    end
  end

  def optional_parser!(data, expected_type, organization)
    if expected_type.present?
      raise "Missing embedded #{expected_type} condition parser" unless data.is_a?(Hash)

      type = require_key!(data, 'type', 'condition parser')
      raise "Condition parser type mismatch: expected #{expected_type.inspect}, got #{type.inspect}" unless type == expected_type

      allowed = PARSER_ATTRIBUTES[type]
      raise "Unsupported condition parser type #{type.inspect}" unless allowed

      type.constantize.create!(safe_attributes(data, allowed, type).merge('organisation_id' => organization.id))
    elsif data.present?
      raise 'Embedded condition parser is present without condition_parser_type'
    end
  end

  def optional_reference!(key, records, label)
    return if key.blank?

    records.fetch(key) { raise "Unknown #{label} key #{key.inspect}" }
  end

  def safe_specific_attributes(data, allowed, task)
    safe_attributes(data, allowed, "#{task.task_type} for #{task.name}")
  end

  def safe_attributes(container, allowed, label)
    exact_attributes!(hash!(container, 'attributes'), allowed, "#{label} attributes")
  end

  def exact_attributes!(attributes, allowed, label)
    raise "#{label} must be a JSON object" unless attributes.is_a?(Hash)

    unknown = attributes.keys - allowed
    raise "#{label} contains unsupported fields: #{unknown.join(', ')}" if unknown.any?

    attributes.slice(*allowed)
  end

  def validate_loaded_configuration!(manifest, organization)
    expected = manifest.fetch('expected_counts')
    actual = database_counts(organization)
    raise "Loaded record counts mismatch: expected #{expected.inspect}, got #{actual.inspect}" unless actual == expected

    workflows = List.where(organisation_id: organization.id).includes(:tasks).to_a
    workflows.each do |list|
      async_count = list.tasks.count { |task| task.task_type == 'AsyncProcessTask' }
      raise "Loaded List #{list.name.inspect} has #{async_count} AsyncProcessTasks" unless async_count == 1
    end

    Task.where(organisation_id: organization.id).find_each do |task|
      klass = task.task_type.safe_constantize
      specific = klass&.unscoped&.find_by(id: task.task_type_id)
      unless specific && specific.task_id.to_s == task.id.to_s && specific.organisation_id == organization.id
        raise "Task #{task.name.inspect} has an invalid generic-to-specific link"
      end
      reverse_matches = klass.unscoped.where(task_id: task.id).to_a
      unless reverse_matches.one? && reverse_matches.first.id == task.task_type_id
        raise "Task #{task.name.inspect} has an invalid specific-to-generic link"
      end
    end
    actual
  end

  def database_counts(organization)
    task_count = Task.where(organisation_id: organization.id).count
    specific_count = Task.task_types.keys.sum do |type|
      type.constantize.unscoped.where(organisation_id: organization.id).count
    end
    {
      'organizations' => Organisation.where(id: organization.id).count,
      'themes' => Theme.where(organisation_id: organization.id).count,
      'workflows' => List.where(organisation_id: organization.id).count,
      'tasks' => task_count,
      'specific_tasks' => specific_count,
      'tags' => Tag.where(organisation_id: organization.id).count,
      'rules' => Rule.where(organisation_id: organization.id).count,
      'rule_conformity_examples' => RuleConformityExample.where(organisation_id: organization.id).count,
      'rule_non_conformity_examples' => RuleNonConformityExample.where(organisation_id: organization.id).count,
      'local_sources' => LocalSource.where(organisation_id: organization.id).count,
      'api_sources' => ApiSource.where(organisation_id: organization.id).count,
      'sftp_sources' => SftpSource.where(organisation_id: organization.id).count,
      'date_parsers' => DateParser.where(organisation_id: organization.id).count,
      'text_parsers' => TextParser.where(organisation_id: organization.id).count
    }
  end

  def manifest_counts(manifest)
    tasks = manifest.fetch('workflows').flat_map { |workflow| array!(workflow, 'tasks') }
    specifics = tasks.map { |task| hash!(task, 'specific_task') }
    rules = manifest.fetch('rules')
    sources = specifics.flat_map { |specific| [specific['local_source'], specific['data_source']] }.compact
    parsers = specifics.filter_map { |specific| specific['condition_parser'] }
    {
      'organizations' => 1,
      'themes' => manifest.fetch('themes').length,
      'workflows' => manifest.fetch('workflows').length,
      'tasks' => tasks.length,
      'specific_tasks' => specifics.length,
      'tags' => manifest.fetch('tags').length,
      'rules' => rules.length,
      'rule_conformity_examples' => rules.sum { |rule| array!(rule, 'conformity_examples').length },
      'rule_non_conformity_examples' => rules.sum { |rule| array!(rule, 'non_conformity_examples').length },
      'local_sources' => sources.count { |source| source['type'] == 'LocalSource' },
      'api_sources' => sources.count { |source| source['type'] == 'ApiSource' },
      'sftp_sources' => sources.count { |source| source['type'] == 'SftpSource' },
      'date_parsers' => parsers.count { |parser| parser['type'] == 'DateParser' },
      'text_parsers' => parsers.count { |parser| parser['type'] == 'TextParser' }
    }
  end

  def index_by_key!(entries, label)
    entries.each_with_object({}) do |entry, result|
      raise "#{label.capitalize} entry must be a JSON object" unless entry.is_a?(Hash)

      key = require_key!(entry, 'key', label)
      raise "Duplicate #{label} key #{key.inspect}" if result.key?(key)

      result[key] = entry
    end
  end

  def require_key!(hash, key, label)
    value = hash[key]
    raise "#{label.capitalize} is missing non-empty #{key}" unless value.is_a?(String) && value.present?

    value
  end

  def array!(hash, key)
    value = hash[key]
    raise "#{key} must be a JSON array" unless value.is_a?(Array)

    value
  end

  def hash!(hash, key)
    value = hash[key]
    raise "#{key} must be a JSON object" unless value.is_a?(Hash)

    value
  end

  def schedules_disabled?
    ENV.fetch('ROXCHAOS_DISABLE_SCHEDULES', '1') == '1'
  end

  def write_json(payload)
    $stdout.write("#{MARKER_BEGIN}\n")
    $stdout.write(JSON.pretty_generate(payload))
    $stdout.write("\n#{MARKER_END}\n")
  end
end

begin
  RoxChaosLoadManifest.run
rescue StandardError => e
  warn "RoxChaos load failed: #{e.message}"
  exit 1
end
