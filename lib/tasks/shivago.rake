HEADERS = ['v=0.1','a|g:name','d:name:imei:account_id:gateway_id','e:name:gateway_id','r:device_id:event_id:latitude:longitude:ignition:speed:created_at']

namespace :shivago do

  MAX_READINGS_PER_CSV = 1024 * 1024
  INPROGRESS_EXPORT_CSV = 'shivago.tmp'
  RECOVERY_FILE = 'shivago.yml'
  KILL_FILE = 'shivago.kill'
  DATE_TOO_OLD = Time.mktime(2000,1,1)
  DATE_TOO_NEW = Time.now.advance(:days => 1)

  task :check => :environment do
    setup_alternative_connection
    csv_count,last_reading_id = recover_previous_export
    reading_min,reading_max,reading_range = check_reading_min_max_range(last_reading_id)
    shivago_logger.info "last reading ID: #{reading_max}"
  end

  desc 'export readings to file shivago.csv'
  task :export => :environment do
    require 'csv'

    setup_alternative_connection

    reset_thread_cache

    csv,csv_filename,reading_count,total_readings,last_reading = nil,nil,0,0,nil
    csv_count,last_reading_id = recover_previous_export
    begin

      enumerate_all_readings(last_reading_id) do |reading,reading_range|
        total_readings += 1
        if (reading_count += 1) > MAX_READINGS_PER_CSV
          cleanup_export_csv(csv,csv_filename,last_reading)
          csv,csv_filename,csv_count,reading_count = nil,nil,csv_count + 1,0
        end

        csv,csv_filename = setup_export_csv(csv_count,reading_range) unless csv

        shivago_logger.info "...#{capture_datetime(Time.now)}: [#{csv_count}] #{reading_count / 1024}K readings" if reading_count % 10240 == 0 and reading_count != 0

        capture_reading(csv,last_reading = reading)
      end

      cleanup_export_csv(csv,csv_filename,last_reading) if csv

    rescue
      shivago_logger.info "ERROR:#{$!}"
      $@.each{|line| shivago_logger.info line}
    ensure
      capture_recovery_info(csv_count,last_reading || last_reading_id) if total_readings > 0
    end

  end

  def reset_thread_cache
    $account_cache,$device_cache,$gateway_cache,$event_cache,$account_count,$device_count,$gateway_count,$event_count = {},{},{},{},0,0,0,0
  end

  def recover_previous_export
    require 'yaml'

    csv_count,last_reading_id = 0,ENV['start_after']
    if File.exists? RECOVERY_FILE
      recovery_info = YAML::load_file RECOVERY_FILE
      $account_cache = recovery_info[:account_cache]
      $device_cache = recovery_info[:device_cache]
      $gateway_cache = recovery_info[:gateway_cache]
      $event_cache = recovery_info[:event_cache]
      $account_count = recovery_info[:account_count]
      $device_count = recovery_info[:device_count]
      $gateway_count = recovery_info[:gateway_count]
      $event_count = recovery_info[:event_count]
      csv_count = recovery_info[:csv_count].to_i + 1
      last_reading_id ||= recovery_info[:last_reading_id]
    end
    [csv_count,last_reading_id.to_s.length == 0 ? nil : last_reading_id.to_i]
  end

  def capture_recovery_info(csv_count,last_reading)
    require 'yaml'

    File.open(RECOVERY_FILE,'w') do |file|
      file.write YAML::dump(
        :account_cache => $account_cache,
        :device_cache => $device_cache,
        :gateway_cache => $gateway_cache,
        :event_cache => $event_cache,
        :account_count => $account_count,
        :device_count => $device_count,
        :gateway_count => $gateway_count,
        :event_count => $event_count,
        :csv_count => csv_count,
        :last_reading_id => last_reading.to_param)
    end
  end

  def setup_alternative_connection
    (connection_spec = alternative_connection_spec) && ActiveRecord::Base.establish_connection(connection_spec)
  end

  def alternative_connection_spec
    raise 'no configuration found' unless connection_spec = ActiveRecord::Base.configurations[Rails.env]

    override_spec = false
    ['adapter','encoding','database','username','password','host'].each do | key |
      next unless ENV.include?(key)

      connection_spec[key] = ENV[key]
      override_spec = true
    end

    connection_spec if override_spec
  end

  def setup_export_csv(csv_count,reading_range)
    csv = CSV.open(INPROGRESS_EXPORT_CSV,'w')
    csv << HEADERS

    csv_filename = make_export_filename(csv_count,reading_range)
    shivago_logger.info "...#{capture_datetime(Time.now)}: start #{csv_filename}"

    [csv,csv_filename]
  end

  def cleanup_export_csv(csv,csv_filename,last_reading)
    csv << ['f',last_reading.to_param] if last_reading
    csv.close
    `mv #{INPROGRESS_EXPORT_CSV} #{csv_filename}`

    shivago_logger.info "...#{capture_datetime(Time.now)}: stop  #{csv_filename}"
  end

  def make_export_filename(counter,reading_range)
    suffix = format("_%0#{[Math.log10([reading_range / MAX_READINGS_PER_CSV,1].max).ceil,1].max}d",counter) if reading_range > MAX_READINGS_PER_CSV or counter > 0
    if prefix = ENV['export']
      Time.now.strftime("#{prefix}_%Y%m%d#{suffix}.csv")
    else
      "shivago#{suffix}.csv"
    end
  end

  def enumerate_all_readings(last_reading_id,&block)
    reading_min,reading_max,reading_range = check_reading_min_max_range(last_reading_id)
    return if reading_range == 0

    if Reading.respond_to? :find_each
      conditions = "id > #{last_reading_id}" if last_reading_id
      Reading.find_each(:conditions => conditions) do |reading|
        return if check_kill_file

        block.call(reading,reading_range)
      end
    else
      while reading_min < reading_max
        return if check_kill_file

        reading_next = reading_min + 1000
        Reading.find_by_sql("select * from readings where id >= #{reading_min} and id < #{reading_next}").each do |reading|
          return if check_kill_file

          block.call(reading,reading_range)
        end
        reading_min = reading_next
      end
    end
  end

  def check_reading_min_max_range(last_reading_id)
    stats = Reading.connection.select_one 'select min(id) min_id,max(id) max_id from readings'
    reading_min = stats['min_id'].to_i
    reading_min = [reading_min,last_reading_id + 1].max if last_reading_id
    reading_max = stats['max_id'].to_i
    if (reading_range = [reading_max - reading_min,0].max) == 0
      shivago_logger.info("no readings found")
    else
      prefix,suffix,order = reading_range,nil,0
      ['','K','M','G','T'].each do |entry|
        suffix = entry
        break if (next_prefix = prefix / 1024) == 0

        prefix = next_prefix
        order += 1
      end
      shivago_logger.info "approximately #{prefix}#{suffix} readings..."
    end

    [reading_min,reading_max,reading_range]
  end

  def check_kill_file
    return false unless File.exists?(KILL_FILE)

    shivago_logger.info 'KILLING PROCESS'
    `rm #{KILL_FILE}`
    true
  end

  def capture_reading(csv,reading)
    return unless reading.created_at and reading.created_at > DATE_TOO_OLD and reading.created_at < DATE_TOO_NEW
    return unless device_index = capture_device_from_reading(csv,reading)

    return unless event_index = grab_cache_entry($event_cache,reading.event_type) do |key|
      csv << ['e',key,grab_cache_entry($gateway_cache,reading.device.gateway_name){0}]
      $event_count += 1
    end

    csv << ['r',device_index,event_index,
      reading.attributes['latitude'],
      reading.attributes['longitude'],
      capture_boolean(reading.attributes['ignition']),
      capture_integer(reading.attributes['speed']),
      capture_datetime(reading.created_at)]
  end

  def capture_boolean(value)
    value.nil? ? nil : value ? 1 : 0
  end

  def capture_integer(value)
    value && value.to_i
  end

  def capture_datetime(value)
    value && value.strftime('%Y-%m-%d %H:%M:%S')
  end

  def capture_device_from_reading(csv,reading)
    return unless reading.device_id and reading.device_id > 0

    grab_cache_entry($device_cache,reading.device_id) do
      return unless device = reading.device

      return unless account_index = grab_cache_entry($account_cache,device.account_id) do
        csv << ['a',(device.account && device.account.company) || (device.account_id.to_i == 0 ? 'none' : "unknown: #{device.account_id}")]
        $account_count += 1
      end

      return unless gateway_index = grab_cache_entry($gateway_cache,device.gateway_name) do |key|
        csv << ['g',key]
        $gateway_count += 1
      end

      csv << ['d',device.name || device.imei || "unknown: #{device.id}",device.imei || "unknown: #{device.id}",account_index,gateway_index]
      $device_count += 1
    end
  end

  def grab_cache_entry(cache,key,&generator)
    key ||= 'none'
    cache[key] || (cache[key] = generator.call(key))
  end

  def shivago_logger
    return $shivago_logger if $shivago_logger

    STDOUT.sync = true
    $shivago_logger = Logger.new(STDOUT)
  end

# TESTING

  task :test_export_utils => :environment do
    shivago_logger.info "RAILS_ENV: #{Rails.env}"

    setup_alternative_connection

    reading_min,reading_max,reading_range = check_reading_min_max_range(nil)
    make_export_filename(0,reading_range)

    shivago_logger.info "SUCCESS!"
  end

end