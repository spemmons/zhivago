class Period
  
  HOUR_IN_SECONDS = 60 * 60

  def self.nearest_hour(time)
    time.utc
    time.beginning_of_day.advance(:hours => time.hour)
  end

  def self.hour_range(start_time,end_time)
    ((nearest_hour(start_time).to_i / HOUR_IN_SECONDS)..(nearest_hour(end_time).to_i / HOUR_IN_SECONDS)).to_a.collect{|days| Time.at(days * HOUR_IN_SECONDS).utc}
  end

end
