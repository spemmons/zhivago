class CacheHelper

  attr_reader :account_cache,:device_cache,:gateway_cache,:event_cache

  def initialize
    @account_cache,@device_cache,@gateway_cache,@event_cache = {},{},{},{}
  end

  def reset_reading_stats
#    self.reset_reading_stats_for_cache(account_cache)
    self.reset_reading_stats_for_cache(device_cache)
#    self.reset_reading_stats_for_cache(gateway_cache)
#    self.reset_reading_stats_for_cache(event_cache)
  end

  def reset_reading_stats_for_cache(cache)
    cache.each{|key,value| value.reset_reading_stats if value}
  end

end