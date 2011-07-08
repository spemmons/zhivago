namespace :shivago do

  desc 'recalc reading stats for a host'
  task :recalc => :environment do
    raise 'host not found' unless host = resolve_host_from_params

    ActiveRecord::Base.logger = Logger.new(STDOUT)
    shivago_logger.info "...#{capture_datetime(Time.now)}: recalc #{host.name}"
    host.recalc_reading_stats{|counter| shivago_logger.info "...#{capture_datetime(Time.now)}: #{counter / 1024}K readings" if counter % 10240 == 0}
  end

  desc 'delete data associated with a host'
  task :delete => :environment do
    raise 'host not found' unless host = resolve_host_from_params

    ActiveRecord::Base.logger = Logger.new(STDOUT)
    host.destroy
  end

end