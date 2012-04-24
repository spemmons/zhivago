namespace :shivago do

  desc 'calculate daily entropy'
  task :daily_entropy => :environment do
    EntropyStudy.daily_study
  end

end