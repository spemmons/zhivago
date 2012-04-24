namespace :shivago do
  
  REMOTE_DIR = '/opt/ublip/rails/current/'
  REMOTE_RAKE_FILE = "#{REMOTE_DIR}lib/tasks/shivago.rake"
  REMOTE_RECOVERY_FILE = "#{REMOTE_DIR}#{RECOVERY_FILE}"
  REMOTE_KILL_FILE = "#{REMOTE_DIR}#{KILL_FILE}"
  ALIAS_FILE = 'captures/alias.yml'

  desc 'detect the presence of shivago files and processes on a host'
  task :detect do
    host,user,password = collect_remote_params(true)

    if host.kind_of?(String)
      shivago_logger.info ssh_check_remote_process(host,user,password) ? "running!" : "not running"
      shivago_logger.info detect_remote_files(host,user,password) ? "files present" : "files missing"
    else
      host.each do |h|
        shivago_logger.info ssh_check_remote_process(host,user,password) ? "running on #{h}!" : "not running on #{h}"
        shivago_logger.info detect_remote_files(h,user,password) ? "present on #{h}" : "missing on #{h}"
      end
    end
  end

  desc 'update host timezone'
  task :tz do
    host,user,password = collect_remote_params(true)

    all_infos = all_alias_infos

    if host.kind_of?(String)
      shivago_logger.info "...#{all_infos[alias_for_host(host,all_infos)][:timezone] = get_remote_timezone(host,user,password)}"
    else
      host.each do |h|
        shivago_logger.info "...#{h}: #{all_infos[alias_for_host(h,all_infos)][:timezone] = get_remote_timezone(h,user,password)}"
      end
    end

    File.open(ALIAS_FILE,'w'){|file| YAML::dump(all_infos,file)}
  end

  def get_remote_timezone(host,user,password)
    require 'net/ssh'

    Net::SSH.start(host,user,:password => password) do |ssh|
      return (ssh.exec!('date') || '') =~ / \d\d:\d\d:\d\d (\w\w\w) / && $1
    end
  end

  desc 'inject shivago.rake into a target host'
  task :inject do
    inject_host(*collect_remote_params)
  end

  desc 'clean shivago.rake and any other shivago files from target host'
  task :clean do
    clean_host(*collect_remote_params)
  end

  desc 'kill a remote export'
  task :kill do
    require 'net/ssh'

    host,user,password = collect_remote_params

    Net::SSH.start(host,user,:password => password) do |ssh|
      ssh_sudo_exec(ssh,password,"-u ublip touch #{REMOTE_KILL_FILE}")
    end
  end

  desc 'list alias options'
  task :list => :environment do
    puts "ALIAS: #{Array(collect_alias_options).join(',')}"
  end

  desc 'initiate a remote export and download the resulting files'
  task :remote => :environment do
    require 'net/ssh'

    action = ENV['action'] || 'check'

    summarize_start(action) if $summarize_actions = action != 'export'
    
    host,user,password = collect_remote_params(true)
    if host.kind_of?(String)
      execute_remote_action(host,user,password,action)
    else
      host.each_with_index do |h,index|
        shivago_logger.info '' if index > 0
        shivago_logger.info "HOST: #{h} ACTION: #{action}"
        break unless execute_remote_action(h,user,password,action)
      end
    end

    summarize_stop if $summarize_actions
  end

  desc 'download shivago*.csv files from a remote host'
  task :download do
    host,user,password = collect_remote_params

    target,last_reading_id,remote_environment = collect_remote_info(host)

    any_downloads = false
    while true
      any_downloads ||= download_all_csv_files(host,user,password,target)

      break unless ssh_check_remote_process(host,user,password)
      shivago_logger.info '...sleeping'
      sleep 60
    end

    download_file(host,user,password,REMOTE_RECOVERY_FILE,target) if any_downloads
  end

  def collect_alias_options
    case ENV['alias']
      when nil,''
        nil
      when '*'
        all_alias_infos.keys.sort
      when 'imported'
        Host.all.collect{|h| h.name}.sort
      when 'missing'
        infos = all_alias_infos
        (infos.keys - Host.all.collect{|h| h.name}).sort
      when /(.+,.+)/
        $1.split(',').sort
      else
        ENV['alias']
    end
  end

  def collect_remote_params(allow_multiples = false)
    require 'highline/import'

    host = case options = collect_alias_options
      when Array
        raise 'multiple aliases not allowd' unless allow_multiples
        all_alias_infos.select{|k,v| options.include?(k)}.collect{|k,v| v[:host]}.sort
      when String
        all_alias_infos[options][:host]
      else
        ENV['host']
    end
    raise 'no host provided' unless host

    user = ENV['user'] || ENV['USER']
    password = ENV['shivago_password'] || ask('password:'){|q| q.echo = '*'}

    [host,user,password]
  end

  def collect_remote_info(host)
    host_key = alias_for_host(host,all_infos = all_alias_infos)
    host_info = all_infos[host_key] || {}

    target = ENV['target'] || ensure_unique_target(Time.now.strftime("captures/#{host_key || host.split('.')[0]}_%Y%m%d")) + "/"
    last_reading_id = ENV['start_after'] || host_info[:last_reading_id]
    remote_environment = ENV['remote_environment'] || host_info[:remote_environment]

    [target,last_reading_id,remote_environment]
  end

  def ensure_unique_target(target)
    return target unless File.exists?(target)

    target += 'A'
    26.times{return target unless File.exists?(target); target.succ!}
    raise 'too many targets on the same day'
  end

  def store_remote_info(host,target)
    require 'yaml'

    return unless host_key = alias_for_host(host,all_infos = all_alias_infos)

    recovery_file = "#{target}#{RECOVERY_FILE}"
    recovery_info = File.exists?(recovery_file) && YAML::load_file(recovery_file)
    last_reading_id = recovery_info[:last_reading_id] if recovery_info

    return shivago_logger.info "no alias info found for: #{host}" unless host_info = all_infos[host_key]

    host_info[:last_target] = target
    host_info[:last_reading_id] = last_reading_id if last_reading_id.to_i > 0
    
    File.open(ALIAS_FILE,'w'){|file| YAML::dump(all_infos,file)}
  end

  def alias_for_host(host,all_infos = all_alias_infos)
    all_infos.each{|key,settings| return key if settings.kind_of?(Hash) and settings[:host] == host}
    nil
  end

  def all_alias_infos
    require 'yaml'

    YAML::load_file(ALIAS_FILE) || {}
  rescue
    {}
  end

  def detect_remote_files(host,user,password)
    ssh_find_files(host,user,password,"#{REMOTE_DIR}lib/tasks/",/shivago\.rake/).any? || ssh_find_files(host,user,password,REMOTE_DIR,/shivago.*/).any?
  end

  def inject_host(host,user,password)
    require 'net/scp'
    require 'net/ssh'

    puts "...inject rake file into #{host}"
    Net::SCP.start(host,user,:password => password){|scp| scp.upload!('lib/tasks/shivago.rake','.')}

    Net::SSH.start(host,user,:password => password) do |ssh|
      ssh_sudo_exec(ssh,password,'chown ublip:ublip shivago.rake')
      ssh_sudo_exec(ssh,password,"mv shivago.rake #{REMOTE_DIR}lib/tasks")
    end
  end

  def clean_host(host,user,password)
    if ssh_check_remote_process(host,user,password)
      shivago_logger.info 'still running!'
    else
      ssh_find_files(host,user,password,REMOTE_DIR,/shivago\.*/).each do |filename|
        shivago_logger.info "...remove #{filename} on #{host}"
        ssh_remove_file(host,user,password,filename)
      end
      shivago_logger.info "...remove rake file from #{host}"
      ssh_remove_file(host,user,password,REMOTE_RAKE_FILE)
    end
  end

  def execute_remote_action(host,user,password,action)
    continue_processing = true

    target,last_reading_id,remote_environment = collect_remote_info(host)

    inject_host(host,user,password)

    readings_expected = action == 'export'
    execute_remote_session(host,user,password) do |event,info|
      case event
        when :command
          shivago_logger.info "#{capture_datetime(Time.now)}: START REMOTE using #{remote_environment || info} start_after=#{last_reading_id}"
          shivago_logger.info ''

          "cd #{REMOTE_DIR} && sudo -u ublip rake RAILS_ENV=#{remote_environment || info} shivago:#{action} start_after=#{last_reading_id}"
        when :stdout
          case info
            when /KILLING PROCESS/
              continue_processing = false
            when /: stop /
              download_all_csv_files(host,user,password,target)
            when /no readings found/
              readings_expected = false
          end
        when :stderr
          # do nothing at this time
      end
    end

    shivago_logger.info ''
    shivago_logger.info "#{capture_datetime(Time.now)}: STOP  REMOTE"

    if readings_expected
      download_all_csv_files(host,user,password,target) # just in case, wouldn't want to lose any!
      download_file(host,user,password,REMOTE_RECOVERY_FILE,target)
      store_remote_info(host,target)
    end

    clean_host(host,user,password)

    continue_processing
  end

  def execute_remote_session(host,user,password,&callback)
    Net::SSH.start(host,user,:password => password) do |ssh|
      raise 'no environment found' unless ssh.exec!("cat #{REMOTE_DIR}config/mongrel_cluster.yml | grep environment") =~ /environment: (\w+)/

      remote_environment = $1

      ssh.open_channel do |channel|
        channel.exec(callback.call(:command,remote_environment)) do |ch, success|
          abort "unexpected failure" unless success

          channel.on_data do |ch, data|
            if data =~ /\[sudo\] password for /
              channel.send_data "#{password}\n"
            else
              print "#{host}> #{data}"
              summarize_host_data(host,data) if $summarize_actions
              callback.call(:stdout,data)
            end
          end

          channel.on_extended_data do |ch, type, data|
            print "#{host} ERROR> #{data}"
            callback.call(:stderr,data)
          end
        end
      end
    end
  end

  def download_all_csv_files(host,user,password,target)
    if (filenames = ssh_find_files(host,user,password,REMOTE_DIR,/shivago.*\.csv/)).empty?
      shivago_logger.info 'no CSV files found'
      false
    else
      shivago_logger.info "#{filenames.length} CSV files found"
      filenames.each do |filename|
        download_file(host,user,password,filename,target)
      end
      true
    end
  end

  def download_file(host,user,password,filename,target)
    require 'net/scp'

    shivago_logger.info "...download #{filename} => #{target}"
    Dir.mkdir(target) unless Dir.exist?(target)
    Net::SCP.start(host,user,:password => password){|scp| scp.download!(filename,target)}
    ssh_remove_file(host,user,password,filename)
  end

  def ssh_check_remote_process(host,user,password)
    require 'net/ssh'

    Net::SSH.start(host,user,:password => password) do |ssh|
      result = ssh.exec!('ps aux | grep shivago')
      return result && result =~ /shivago:export/
    end
  end

  def ssh_find_files(host,user,password,directory,pattern)
    require 'net/ssh'

    Net::SSH.start(host,user,:password => password) do |ssh|
      return ssh.exec!("ls -1 #{directory}").split("\n").select{|filename| filename =~ pattern}.collect{|filename| directory + filename}
    end
  end

  def ssh_remove_file(host,user,password,filename)
    require 'net/ssh'

    Net::SSH.start(host,user,:password => password) do |ssh|
      result = ssh_sudo_exec(ssh,password,"rm #{filename}")
      shivago_logger.info result if result and result.strip!.length > 0
    end
  end

  def ssh_sudo_exec(ssh,password,command)
    result = nil
    ssh.exec! "sudo #{command}" do |channel,stream,data|
      if data =~ /\[sudo\] password for /
        channel.send_data "#{password}\n"
      else
        result = data
      end
    end
    result
  end

  def summarize_start(action)
    File.open('shivago_summary.txt','a') {|file| file.puts("#{Time.now.to_s(:db)} - ACTION: #{action}")}
  end

  def summarize_stop
    File.open('shivago_summary.txt','a') {|file| file.puts("#{Time.now.to_s(:db)} - DONE\n\n")}
  end

  def summarize_host_data(host,data)
    File.open('shivago_summary.txt','a') do |file|
      data.chomp.split("\n").each do |info|
        case info
          when /:/
            file.puts($summarized_host = host) if host != $summarized_host
            file.puts "\t#{info}"
          when /,/
            file.puts "#{alias_for_host(host)},#{info}"
        end
      end
    end
  end

end