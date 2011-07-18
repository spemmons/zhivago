class PeriodicStat < ActiveRecord::Base
  belongs_to :gateway

  EARLIEST_PERIOD = Time.gm(2009,1,1)

  extend ActionView::Helpers::NumberHelper

  def self.reset_all(logger = Logger.new(STDOUT))
    logger.info "...#{Time.now.to_s(:db)}: clearing existing stats"
    delete_all

    device_count,stat_count,period_cache,gateway_cache = 0,0,{},{}
    devices = Device.all(:conditions => ['newest_reading_at > ?',EARLIEST_PERIOD],:order => :oldest_reading_at)
    logger.info "...#{Time.now.to_s(:db)}: collecting stats for #{number_with_delimiter devices.count} devices"
    devices.each do |device|

      logger.info "...#{Time.now.to_s(:db)}: #{device_count / 1024}K lines" if (device_count += 1) % 1024 == 0
      hours = Period.hour_range([device.oldest_reading_at,EARLIEST_PERIOD].max,device.newest_reading_at)
      hours.each do |hour|
        sub_cache = period_cache[hour] ||= {}
        unless stats = sub_cache[device.gateway_id]
          stat_count += 1
          gateway_cache[device.gateway_id] ||= device.gateway
          stats = sub_cache[device.gateway_id] = [0,0,0]
        end
        stats[0] += 1 # NOTE devices_available
      end

      rows = connection.select_rows %(select adddate(from_days(to_days(created_at)),interval hour(created_at) hour),count(*) from readings where device_id = #{device.id} and created_at >= '#{EARLIEST_PERIOD.to_s(:db)}' group by to_days(created_at),hour(created_at))
      rows.each_with_index do |row,row_index|
        raise "period mismatch at for row[#{row_index}] #{row} and device #{device.id}" unless stats = period_cache[row[0]][device.gateway_id]
        stats[1] += 1 # NOTE devices_reported
        stats[2] += row[1] # NOTE readings_sent
      end

    end

    logger.info "...#{Time.now.to_s(:db)}: saving stat data to infile #{number_with_delimiter device_count} available devices with #{number_with_delimiter stat_count} stats"
    infile = Rails.root + 'shivago_stats.txt'
    File.open(infile,'w') do |file|
      period_cache.each do |period,sub_cache|
        sub_cache.each do |gateway_id,stats|
          gateway = gateway_cache[gateway_id]
          file.puts %(#{period.to_s(:db)}\t#{gateway_id}\t#{gateway.host.name}\t#{gateway.name}\t#{stats.join("\t")})
        end
      end
    end
    
    logger.info "...#{Time.now.to_s(:db)}: loading stat data from infile"
    connection.execute "load data infile '#{infile}' into table periodic_stats (starting_at,gateway_id,host_name,gateway_name,devices_available,devices_reported,readings_sent)"
    logger.info "...#{Time.now.to_s(:db)}: done"
  end
end
