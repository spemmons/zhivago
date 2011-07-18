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

  desc 'archive already-imported captures'
  task :archive => :environment do
    Host.all(:order => :name).each do |host|
      host.captures.each do |capture|
        if File.exists?("captures/#{capture.name}.tar.gz")
          shivago_logger.info "already archived #{capture.name}..."
        elsif File.directory?("captures/#{capture.name}/")
          shivago_logger.info "archiving #{capture.name}..."
          files = Dir["captures/#{capture.name}/*"]
          `tar -czf captures/#{capture.name}.tar.gz captures/#{capture.name}/shivago*`
          `tar -tvf captures/#{capture.name}.tar.gz`.split("\n").each do |line|
            next unless line =~ /(captures\/#{capture.name}\/shivago.*)/

            files = files.delete_if{|entry| entry == $1}
          end
          raise "capture not archived: #{capture.name} -- missing: #{files}" unless files.empty?

          `rm -Rf captures/#{capture.name}/`
        else
          raise "archive missing: #{capture.name}..."
        end
      end
    end
  end

end