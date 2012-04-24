namespace :zhivago do

  desc 'calculate geographic stats'
  task :geo_stats => :environment do
    min_reading_at = ENV['min_reading_at'] ? Time.zone.parse(ENV['min_reading_at']) : Time.gm(2011,1,1)
    readings_seen,geo_map = 0,{}
    zhivago_logger.info "...#{Time.now.to_s(:db)}: collect counts"
    Reading.find_each(:conditions => ['created_at > ? and latitude is not null',min_reading_at]) do |reading|
      zhivago_logger.info "...#{Time.now.to_s(:db)}: #{readings_seen / (1024 * 1024)}M readings" if (readings_seen += 1) % (1024 * 1024) == 0
      x,y = (reading.latitude * 10).to_i,(reading.longitude * 10).to_i
      row = geo_map[x] ||= {}
      row[y] = (row[y] || 0) + 1
    end
    zhivago_logger.info "...#{Time.now.to_s(:db)}: create output"
    File.open('geo_stats.csv','w') do |file|
      file.puts 'lat,lon,count'
      geo_map.each do |x,row|
        row.each do |y,count|
          file.puts "#{x},#{y},#{count}"
        end
      end
    end
    zhivago_logger.info "...#{Time.now.to_s(:db)}: done!"
  end

  desc 'calculate periodic stats for hosts by alias'
  task :periodic_stats => :environment do
    case ENV['alias']
      when '*'
        PeriodicStat.reset_all(zhivago_logger)
      when nil
        raise 'host alias not specified'
      when /(.+,.+)/
        $1.split(',').sort.collect{|name| Host.find_by_name(name)}.each{|host| PeriodicStat.reset_host(host)}
      else
        PeriodicStat.reset_host(Host.find_by_name(ENV['alias']))
    end
  end

  DAY_IN_SECONDS = 24 * 60 * 60

  # select timestampdiff(month,oldest_reading_at,now()) age,timestampdiff(month,oldest_reading_at,newest_reading_at) duration,count(*) from devices group by timestampdiff(month,oldest_reading_at,now()),timestampdiff(month,oldest_reading_at,newest_reading_at) order by timestampdiff(month,oldest_reading_at,now()),timestampdiff(month,oldest_reading_at,newest_reading_at)
  # select timestampdiff(month,oldest_reading_at,now()) age,timestampdiff(month,oldest_reading_at,newest_reading_at) duration,count(*) from devices where account_id in (select a.id from accounts a where a.id = account_id and host_id = 24) group by timestampdiff(month,oldest_reading_at,now()),timestampdiff(month,oldest_reading_at,newest_reading_at) order by timestampdiff(month,oldest_reading_at,now()),timestampdiff(month,oldest_reading_at,newest_reading_at)

  desc 'produce aging stats'
  task :age_stats => :environment do

    min_reading_at = ENV['min_reading_at'] ? Time.zone.parse(ENV['min_reading_at']) : Time.gm(2010,5,20)

    zhivago_logger.info "...#{Time.now.to_formatted_s(:db)}: find devices"
    rows = Device.connection.select_rows "select id,oldest_reading_at,newest_reading_at from devices where newest_reading_at > '#{min_reading_at.to_formatted_s(:db)}' order by oldest_reading_at"

    $host_names,$gateway_names,$period_lookup = {},{},{}
    zhivago_logger.info "...#{Time.now.to_formatted_s(:db)}: process #{rows.length} rows"
    rows.each {|row| build_age_range(min_reading_at,*row)}
    rows.each {|row| build_lost_range(min_reading_at,$period_lookup.keys.sort.last,*row)}

    print_dataset('age_stats.csv','_days_or_less')

  end

  def build_age_range(min_reading_at,device_id,oldest_reading_at,newest_reading_at)
    periods_for_time_range(oldest_reading_at,newest_reading_at).each_with_index do |period,day_count|
      next if period < min_reading_at

      bucket = "max" if (bucket = format('%03d',[((day_count / 30) + 1) * 30,370].min)) =~ /370/
      $host_names[bucket] ||= true
      period_data = $period_lookup[period] ||= [0,{},{}]
      period_data[0] += 1
      period_data[1][bucket] = (period_data[1][bucket] || 0) + 1
    end
  end

  def build_lost_range(min_reading_at,final_period,device_id,oldest_reading_at,newest_reading_at)
    return unless (newest_period = period_from_time(newest_reading_at)) < final_period

    periods_for_time_range(newest_period,final_period).each do |period|
      next if period < min_reading_at
      next unless period_data = $period_lookup[period]

      $gateway_names['lost'] ||= true
      period_data[2]['lost'] = (period_data[2]['lost'] || 0) + 1
    end
  end

  desc 'calculate device stats'
  task :device_stats => :environment do

    conditions = ''
    conditions += " and h.name = '#{ENV['alias']}'" unless ENV['alias'].blank?
    conditions += " and #{ENV['conditions']}" unless ENV['conditions'].blank?

    zhivago_logger.info "...#{Time.now.to_formatted_s(:db)}: start '#{conditions}'"
    rows = Device.connection.select_rows "select h.name,g.name,d.id,d.reading_count,d.oldest_reading_at,d.newest_reading_at from hosts h,accounts a,devices d,gateways g where h.id = a.host_id and a.id = account_id and g.id = gateway_id#{conditions} order by d.oldest_reading_at"

    $host_names,$gateway_names,$period_lookup = {},{},{}
    zhivago_logger.info "...#{Time.now.to_formatted_s(:db)}: process #{rows.length} rows"
    rows.each {|row| build_device_data_range(*row)}

    print_dataset('device_stats.csv')

  end

  # select adddate(from_days(to_days(created_at)),interval hour(created_at) hour) period,(select name from hosts where hosts.id = host_id) host,(select name from gateways where gateways.id = gateway_id) gateway,device_id device,count(*) reading_count from readings where created_at >= '2010-05-20' and created_at < '2011-06-01' group by to_days(created_at),hour(created_at),device_id into outfile 'hourly_export.txt'

  desc 'hourly reading stats'
  task :hourly_stats => :environment do
    $host_names,$gateway_names,$period_lookup = {},{},{}
    line_number = 0
    infile = ENV['infile'] || 'hourly_export.txt'
    zhivago_logger.info "...#{Time.now.to_formatted_s(:db)}: start #{infile}"
    File.foreach(infile) do | line |
      zhivago_logger.info "...#{Time.now.to_formatted_s(:db)}: #{line_number / 1024}K lines read from #{infile}" if (line_number += 1) % 10240 == 0

      parts = line.split("\t")
      next if parts[1] == '\N' or parts[2] == '\N' or parts[3] == '\N'

      build_period_data(Time.zone.parse(parts[0]),parts[1],parts[2],parts[3],parts[4].to_i)
    end

    print_dataset('hourly_stats.csv')

  end

  def print_dataset(outfile,suffixes = ['_devices','_readings'])

    $host_names = $host_names.keys.sort
    $gateway_names = $gateway_names.keys.sort
    line_number = 0
    suffixes = Array(suffixes)
    zhivago_logger.info "...#{Time.now.to_formatted_s(:db)}: start output to #{outfile}"
    File.open(outfile,'w') do |file|
      file.print 'period'
      suffixes.each{|suffix| file.print ",all#{suffix}#{$host_names.collect{|h| ",#{h}#{suffix}"}.join}#{$gateway_names.collect{|g|  ",#{g}#{suffix}"}.join}"}
      file.puts ''

      $period_lookup.keys.sort.each do |period|
        zhivago_logger.info "...#{Time.now.to_formatted_s(:db)}: #{line_number / 1024}K lines written to #{outfile}" if (line_number += 1) % 10240 == 0

        period_data = $period_lookup[period]
        file.print period.strftime('%Y-%m-%d %H:%M:%S')
        suffixes.each_with_index{|suffix,index| print_keyed_data(file,period_data,index * 3)}
        file.puts ''
      end
    end
    zhivago_logger.info "...#{Time.now.to_formatted_s(:db)}: #{line_number} lines written to #{outfile}"
  end

  def print_keyed_data(file,period_data,offset)
    file.print ",#{period_data[offset]}"
    print_sub_data(file,period_data,offset + 1,$host_names)
    print_sub_data(file,period_data,offset + 2,$gateway_names)
  end

  def print_sub_data(file,period_data,offset,keys)
    sub_data = period_data[offset]
    keys.each{|k| file.print ",#{sub_data[k]}"}
  end

  def build_device_data_range(host_name,gateway_name,device_id,reading_count,oldest_reading_at,newest_reading_at)
    periods = periods_for_time_range(oldest_reading_at,newest_reading_at)
    case periods.length
      when 1
         build_period_data(periods.first,host_name,gateway_name,device_id,reading_count)
      when 2
        build_device_range_ends(periods.first,periods.last,host_name,gateway_name,device_id,reading_count)
      else
        daily_portion = reading_count.to_f / periods.length
        readings_allocated = build_device_range_ends(periods.first,periods.last,host_name,gateway_name,device_id,daily_portion.to_i)
        reading_accumulation = readings_allocated.to_f
        (1..(periods.length - 2)).each do |index|
          reading_accumulation += daily_portion
          if (current_allocation = reading_accumulation.to_i) >= readings_allocated
            build_period_data(periods[index],host_name,gateway_name,device_id,current_allocation - readings_allocated)
            readings_allocated = current_allocation
          end
        end
    end
  end

  def build_device_range_ends(first_period,last_period,host_name,gateway_name,device_id,reading_count)
    build_period_data(first_period,host_name,gateway_name,device_id,first_half = [reading_count / 2,1].max)
    build_period_data(last_period,host_name,gateway_name,device_id,[reading_count - first_half,1].max)
    first_half
  end

  def build_period_data(period,host_name,gateway_name,device_id,reading_count)
    $host_names[host_name] ||= true
    $gateway_names[gateway_name] ||= true
    period_data = $period_lookup[period] ||= [0,{},{},0,{},{}]
    period_data[0] += 1
    period_data[1][host_name] = (period_data[1][host_name] || 0) + 1
    period_data[2][gateway_name] = (period_data[2][gateway_name] || 0) + 1
    period_data[3] += reading_count
    period_data[4][host_name] = (period_data[4][host_name] || 0) + reading_count
    period_data[5][gateway_name] = (period_data[5][gateway_name] || 0) + reading_count
    period_data
  end

  def periods_for_time_range(start_time,end_time)
    ((start_time.utc.beginning_of_day.to_i / DAY_IN_SECONDS)..(end_time.utc.end_of_day.to_i / DAY_IN_SECONDS)).to_a.collect{|days| Time.at(days * DAY_IN_SECONDS).utc}
  end

  def period_from_time(time)
    time.utc.beginning_of_day
  end

# TESTING

  task :test_stats_utils => :environment do
    $host_names,$gateway_names,$period_lookup = {},{},{}

    stats_assert_equal 'period_from_time',Time.gm(2000,1,1),period_from_time(Time.gm(2000,1,1,12,15))
    stats_assert_equal 'periods_for_time_range same day',[Time.gm(2000,1,1)],periods_for_time_range(Time.gm(2000,1,1,12,15),Time.gm(2000,1,1,12,30))
    stats_assert_equal 'periods_for_time_range span 2 days',[Time.gm(2000,1,1),Time.gm(2000,1,2)],periods_for_time_range(Time.gm(2000,1,1,12,15),Time.gm(2000,1,2,12,30))
    stats_assert_equal 'periods_for_time_range span 23 days',[Time.gm(2000,1,1),Time.gm(2000,1,2),Time.gm(2000,1,3)],periods_for_time_range(Time.gm(2000,1,1,12,15),Time.gm(2000,1,3,12,30))

    stats_assert_equal 'build_period_data initial values',[1,{'host' => 1},{'gateway' => 1},100,{'host' => 100},{'gateway' => 100}],build_period_data(Time.gm(2000,1,1),'host','gateway','imei',100)
    stats_assert_equal 'build_period_data additional values',[2,{'host' => 2},{'gateway' => 2},150,{'host' => 150},{'gateway' => 150}],build_period_data(Time.gm(2000,1,1),'host','gateway','imei',50)
    stats_assert_equal 'build_device_data_range length 1',[3,{'host' => 3},{'gateway' => 3},175,{'host' => 175},{'gateway' => 175}],build_device_data_range('host','gateway','imei',25,Time.gm(2000,1,1,12,15),Time.gm(2000,1,1,12,30))

    build_device_data_range('host','gateway','imei',3,Time.gm(2000,1,1,12,15),Time.gm(2000,1,2,12,30))
    stats_assert_equal 'build_device_data_range length 2',({
        Time.gm(2000,1,1) => [4,{'host' => 4},{'gateway' => 4},176,{'host' => 176},{'gateway' => 176}],
        Time.gm(2000,1,2) => [1,{'host' => 1},{'gateway' => 1},2,{'host' => 2},{'gateway' => 2}]}),$period_lookup

    build_device_data_range('host','gateway','imei',3,Time.gm(2000,1,1,12,15),Time.gm(2000,1,3,12,30))
    stats_assert_equal 'build_device_data_range length 3',({
        Time.gm(2000,1,1) => [5,{'host' => 5},{'gateway' => 5},177,{'host' => 177},{'gateway' => 177}],
        Time.gm(2000,1,2) => [2,{'host' => 2},{'gateway' => 2},3,{'host' => 3},{'gateway' => 3}],
        Time.gm(2000,1,3) => [1,{'host' => 1},{'gateway' => 1},1,{'host' => 1},{'gateway' => 1}]}),$period_lookup

    $host_names,$gateway_names,$period_lookup = {},{},{}

    build_device_data_range('host','gateway','imei',4,Time.gm(2000,1,1),Time.gm(2000,1,12))
    stats_assert_equal 'intersperse infrequent readings',({
        Time.gm(2000,1,1) => [1,{'host' => 1},{'gateway' => 1},1,{'host' => 1},{'gateway' => 1}],
        Time.gm(2000,1,2) => [1,{'host' => 1},{'gateway' => 1},0,{'host' => 0},{'gateway' => 0}],
        Time.gm(2000,1,3) => [1,{'host' => 1},{'gateway' => 1},0,{'host' => 0},{'gateway' => 0}],
        Time.gm(2000,1,4) => [1,{'host' => 1},{'gateway' => 1},0,{'host' => 0},{'gateway' => 0}],
        Time.gm(2000,1,5) => [1,{'host' => 1},{'gateway' => 1},1,{'host' => 1},{'gateway' => 1}],
        Time.gm(2000,1,6) => [1,{'host' => 1},{'gateway' => 1},0,{'host' => 0},{'gateway' => 0}],
        Time.gm(2000,1,7) => [1,{'host' => 1},{'gateway' => 1},1,{'host' => 1},{'gateway' => 1}],
        Time.gm(2000,1,8) => [1,{'host' => 1},{'gateway' => 1},0,{'host' => 0},{'gateway' => 0}],
        Time.gm(2000,1,9) => [1,{'host' => 1},{'gateway' => 1},0,{'host' => 0},{'gateway' => 0}],
        Time.gm(2000,1,10) => [1,{'host' => 1},{'gateway' => 1},1,{'host' => 1},{'gateway' => 1}],
        Time.gm(2000,1,11) => [1,{'host' => 1},{'gateway' => 1},0,{'host' => 0},{'gateway' => 0}],
        Time.gm(2000,1,12) => [1,{'host' => 1},{'gateway' => 1},1,{'host' => 1},{'gateway' => 1}],
        }),$period_lookup

    puts 'SUCCESS!'
  end

  def stats_assert_equal(label,expected,actual)
    raise "#{label}\n\t expected: #{expected}\n\tactual: #{actual}" unless expected == actual
  end

  def stats_assert(label,success)
    raise label unless success
  end

end