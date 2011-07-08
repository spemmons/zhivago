namespace :shivago do

  desc 'refresh the already-imported hosts with new captures'
  task :refresh => :environment do
    Host.all(:order => :name).each_with_index do |host,index|
      shivago_logger.info '' if index > 0
      shivago_logger.info "update #{host.name}..."
      import_host(host)
    end
  end

  desc 'import captured readings from shivago*.csv or import=... ENV value'
  task :import => :environment do
    raise 'host not found' unless host = resolve_host_from_params

    import_host(host)
  end

  def import_host(host)
    past_capture_names = host ? host.captures.all.collect{|capture| capture.name} : []
    available_capture_names = Dir["captures/#{host.name}*"].collect{|directory| directory =~ /captures\/(.*)/ && $1}
    unprocessed_capture_names = available_capture_names - past_capture_names
    if unprocessed_capture_names.empty?
      shivago_logger.info "no unprocessed captures for #{host.name}"
    else
      unprocessed_capture_names.each{|capture_directory| Capture.import_for_host(host.name,host.timezone,capture_directory)}
    end
  end

  def resolve_host_from_params
    raise "no host alias given" unless host_alias = ENV['alias']
    raize "host has not timezone" unless (host_info = all_alias_infos[host_alias]) && (timezone = host_info[:timezone])

    Host.find_by_name(host_alias) || Host.new(:name => host_alias,:timezone => timezone)
  end

# TESTING

  task :test_import_utils => :environment do
  end

end