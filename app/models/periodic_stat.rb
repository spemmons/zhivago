class PeriodicStat < ActiveRecord::Base
  belongs_to :gateway

  EARLIEST_PERIOD = Time.gm(2009,1,1)

  extend ActionView::Helpers::NumberHelper

  def self.reset_all(logger = Logger.new(STDOUT))
    self.delete_all
    self.reset_gateway_scope(Gateway.all(:join => :host,:order => 'hosts.name,gateways.name'),logger)
  end

  def self.reset_host(host,logger = Logger.new(STDOUT))
    self.reset_gateway_scope(host.gateways(:order => :name),logger)
  end

  def self.reset_gateway_scope(gateway_scope,logger)
    gateway_scope.each{|gateway| self.reset_gateway(gateway,logger)}
  end

  def self.reset_gateway(gateway,logger)
    logger.info "...#{Time.now.to_s(:db)}: clearing existing stats for host #{gateway.host.name}/#{gateway.name} [#{gateway.id}]"
    gateway.periodic_stats.delete_all

    device_count,stat_count,period_cache = 0,0,{}
    devices = gateway.devices.all(:conditions => ['newest_reading_at > ?',EARLIEST_PERIOD],:order => :oldest_reading_at)
    logger.info "...#{Time.now.to_s(:db)}: collecting stats for #{number_with_delimiter devices.count} devices"
    devices.each do |device|

      logger.info "...#{Time.now.to_s(:db)}: #{device_count / 1024}K lines" if (device_count += 1) % 1024 == 0
      hours = Period.hour_range([device.oldest_reading_at,EARLIEST_PERIOD].max,device.newest_reading_at)
      hours.each do |hour|
        unless stats = period_cache[hour]
          stat_count += 1
          stats = period_cache[hour] = [0,0,0]
        end
        stats[0] += 1       # NOTE devices_available
      end

      rows = connection.select_rows %(select adddate(from_days(to_days(created_at)),interval hour(created_at) hour),count(*) from readings where device_id = #{device.id} and created_at >= '#{EARLIEST_PERIOD.to_s(:db)}' group by to_days(created_at),hour(created_at))
      rows.each_with_index do |row,row_index|
        raise "period mismatch at for row[#{row_index}] #{row} and device #{device.id}" unless stats = period_cache[row[0]]
        stats[1] += 1       # NOTE devices_reported
        stats[2] += row[1]  # NOTE readings_sent
      end

    end

    logger.info "...#{Time.now.to_s(:db)}: saving stat data to infile #{number_with_delimiter device_count} available devices with #{number_with_delimiter stat_count} stats"
    infile = Rails.root + "shivago_stats_#{gateway.id}.txt"
    File.open(infile,'w') do |file|
      period_cache.each do |period,stats|
        file.puts %(#{period.to_s(:db)}\t#{gateway.id}\t#{gateway.host.name}\t#{gateway.name}\t#{stats.join("\t")})
      end
    end

    logger.info "...#{Time.now.to_s(:db)}: loading stat data from infile"
    connection.execute "load data infile '#{infile}' into table periodic_stats (starting_at,gateway_id,host_name,gateway_name,devices_available,devices_reported,readings_sent)"
    logger.info "...#{Time.now.to_s(:db)}: done"
  end
end
