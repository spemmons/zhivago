class Capture < ActiveRecord::Base
  belongs_to :host

  HEADERS ||= eval(File.readlines('lib/tasks/shivago.rake')[0] =~ /HEADERS = (.*)/ && $1)
  DATE_TOO_OLD = Time.mktime(2000,1,1)
  DATE_TOO_NEW = Time.now.advance(:days => 1)

  has_many :accounts,:dependent => :delete_all
  has_many :devices,:dependent => :delete_all
  has_many :gateways,:dependent => :delete_all
  has_many :events,:dependent => :delete_all
  has_many :created_readings,:class_name => Reading.to_s,:dependent => :delete_all # NOTE: this is here to allow deletion

  include ReadingStats

  def initialize(params)
    super(params)
    @account_lookup,@device_lookup,@gateway_lookup,@event_lookup = [nil],[nil],[nil],[nil]
    @cache_helper,@fix_event_snafu_cache = CacheHelper.new,[]
  end

# public methods

  def self.for_host(host_alias,timezone = nil,capture_directory = nil)
    result = create!(:name => capture_directory)
    result.host = result.find_or_create_by_name(Host,:name => host_alias,:timezone => timezone)
    result.save!
    result
  end
  
  def self.import_for_host(host_alias,timezone,capture_directory,logger = Logger.new(STDOUT))
    logger.info "import #{capture_directory} for #{host_alias}"
    capture = Capture.for_host(host_alias,timezone,capture_directory)
    capture.logger = logger
    if capture.host.timezone.nil?
      logger.info "...host has no timezone"
    elsif (capture_files = Dir["captures/#{capture_directory}/shivago*.csv"].sort).empty?
      logger.info "...no CSV files for capture"
    else
      capture_files.each{|capture_file| capture.import_capture_file(capture_file)}
      capture.load_imported_readings
    end
    capture
  end

  def logger
    @logger ||= Logger.new(STDOUT)
  end

  def logger=(value)
    @logger = value
  end

  def import_capture_file(capture_file)
    require 'csv'

    logger.info "...#{Time.now.to_s(:db)}: processing #{capture_file}"
    line_number = 0
    CSV.foreach(capture_file) do | line |
      logger.info "...#{Time.now.to_s(:db)}: #{line_number / 1024}K lines" if (line_number += 1) % 10240 == 0
      if line_number > 1
        case line[0]
          when 'a'
            note_account(line[1])
          when 'e'
            note_event(line[1],line[2].to_i)
          when 'g'
            note_gateway(line[1])
          when 'd'
            note_device(line[1],line[2],line[3].to_i,line[4].to_i)
          when 'r'
            if line[7].blank? or (created_at = Time.parse("#{line[7]} #{self.host.timezone}").utc) < DATE_TOO_OLD or created_at > DATE_TOO_NEW
              logger.info "...#{Time.now.to_s(:db)}: invalid date: #{line[7]} at #{line_number}"
            else
              create_reading(line[1].to_i,line[2].to_i,line[3],line[4],line[5],line[6],created_at)
            end
          when 'f'
            logger.info "...#{Time.now.to_s(:db)}: final reading ID:#{line[1]}"
          else
            logger.info "...#{Time.now.to_s(:db)}: unexpected entry at #{line_number}: #{line.inspect}"
          end
      elsif line != HEADERS
        raise 'headers do not match'
      end
    end
    logger.info "...#{Time.now.to_s(:db)}: finished #{capture_file}"
  end

  def load_imported_readings
    return unless @readings_infile

    @readings_infile.close
    @readings_infile = nil

    logger.info "...#{Time.now.to_s(:db)}: loading #{@readings_imported} readings from '#{@readings_infile_filename}'"
    self.class.connection.execute "load data infile '#{@readings_infile_filename}' into table readings"

    first_imported_reading_id = self.class.connection.select_value "select last_insert_id()"
    last_imported_reading_id = first_imported_reading_id + @readings_imported - 1

    File.delete @readings_infile_filename

    logger.info "...#{Time.now.to_s(:db)}: reset capture stats"
    self.reading_count += @readings_imported
    self.first_reading = Reading.find(first_imported_reading_id) if self.first_reading_id == 0
    self.last_reading = Reading.find(last_imported_reading_id)
    self.save!

    logger.info "...#{Time.now.to_s(:db)}: reset host stats"
    self.host.reset_reading_stats

    logger.info "...#{Time.now.to_s(:db)}: reset other stats"
    @cache_helper.reset_reading_stats

    logger.info "...#{Time.now.to_s(:db)}: finished #{@readings_imported} readings"
    [first_imported_reading_id,last_imported_reading_id]
  end

  def note_account(name)
    find_or_create_account(@account_lookup.length,name)
  end

  def lookup_account(index)
    find_or_create_account(index)
  end

  def note_gateway(name)
    find_or_create_gateway(@gateway_lookup.length,name)
  end

  def lookup_gateway(index)
    find_or_create_gateway(index)
  end

  def note_event(name,gateway_index)
    find_or_create_event(@event_lookup.length,name,gateway_index)
  end

  def lookup_event(event_index)
    find_or_create_event(event_index)
  end
  
  def note_device(name,imei,account_index,gateway_index)
    find_or_create_device(@device_lookup.length,name,imei,account_index,gateway_index)
  end

  def lookup_device(device_index)
    find_or_create_device(device_index)
  end

  def create_reading(device_index,event_index,latitude,longitude,ignition,speed,created_at)
    device = lookup_device(device_index)
    event = fix_event_snafu(device,event_index)

    @readings_infile,@readings_imported = File.open(@readings_infile_filename = Rails.root + "captures/#{self.name}/readings.txt",'w'),0 unless @readings_infile
    @readings_infile.puts "\\N\t#{self.to_param}\t#{self.host.to_param}\t#{device.account.to_param}\t#{device.to_param}\t#{event.gateway.to_param}\t#{event.to_param}\t#{infile_value(latitude)}\t#{infile_value(longitude)}\t#{infile_value(ignition)}\t#{infile_value(speed)}\t#{created_at.to_s(:db)}\t#{Time.now.to_s(:db)}"
    @readings_imported += 1

    self.oldest_reading_at = created_at unless self.oldest_reading_at and self.oldest_reading_at < created_at
    self.newest_reading_at = created_at unless self.newest_reading_at and self.newest_reading_at > created_at
  end
  
# helper methods

  # NOTE shivago:export failed to properly identify events with the same name from different gateways
  def fix_event_snafu(device,event_index)
    if (event = lookup_event(event_index)).gateway_id != device.gateway_id
      second_level_cache = @fix_event_snafu_cache[device.gateway_id]
      unless result = second_level_cache[event.name]
        result = find_or_create_by_params(Event,:gateway_id => device.gateway.to_param,:name => event.name)
        second_level_cache[result.name] = result
        @cache_helper.event_cache[result.id] = result
      end
      event = result
    end
    event
  end

  def find_or_create_account(account_index,name = nil)
    find_or_create_in_lookup(@account_lookup,@cache_helper.account_cache,account_index){find_or_create_by_params(Account,:host_id => self.host.to_param,:name => name || 'none')}
  end

  def find_or_create_gateway(gateway_index,name = nil)
    find_or_create_in_lookup(@gateway_lookup,@cache_helper.gateway_cache,gateway_index) do
      gateway = find_or_create_by_params(Gateway,:host_id => self.host.to_param,:name => name || 'none')
      @fix_event_snafu_cache[gateway.id] = {}
      gateway
    end
  end

  def find_or_create_event(event_index,name = nil,gateway_index = 0)
    find_or_create_in_lookup(@event_lookup,@cache_helper.event_cache,event_index) do
      gateway = lookup_gateway(gateway_index)
      event = find_or_create_by_params(Event,:gateway_id => gateway.to_param,:name => name || 'none')
      @fix_event_snafu_cache[event.gateway_id][event.name] = event
      event
    end
  end

  def find_or_create_device(device_index,name = nil,imei = nil,account_index = 0,gateway_index = 0)
    find_or_create_in_lookup(@device_lookup,@cache_helper.device_cache,device_index) do
      imei ||= 'none'
      find_or_create_by_keys(Device,[:imei,:account_id],:account_id => lookup_account(account_index).to_param,:gateway_id => lookup_gateway(gateway_index).to_param,:imei => imei,:name => name || imei)
    end
  end

  def find_or_create_in_lookup(lookup,cache,index,&factory)
    unless result = lookup[index]
      lookup[index] = result = factory.call
      cache[result.id] = result
    end
    result
  end

  def find_or_create_by_name(klass,params)
    find_or_create_by_keys(klass,:name,normalize_name_params(params))
  end

  def find_or_create_by_params(klass,params)
    find_or_create_by_keys(klass,params.keys,params)
  end

  def find_or_create_by_keys(klass,keys,params)
    keys = Array(keys)
    params.each{|k,v| v.force_encoding('ISO-8859-1').encode!('UTF-8',invalid: :replace,undef: :replace,replace: '?') if v} # HACK fix invalid encoding problem: "359138032584\235\00016".blank?
    result = klass.first(:conditions => params.select{|k| keys.include?(k)})
    eval "self.#{klass.table_name}_#{result ? 'updated' : 'created'} += 1"
    result || klass.create!(params.merge(:capture_id => self.to_param))
  end

  def infile_value(value)
    value ? value : "\\N"
  end

  def normalize_name_params(params)
    params = {:name => params} if params.kind_of?(String)
    params
  end
end
