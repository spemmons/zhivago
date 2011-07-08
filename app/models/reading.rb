class Reading < ActiveRecord::Base
  belongs_to :capture
  belongs_to :host
  belongs_to :account
  belongs_to :device
  belongs_to :gateway
  belongs_to :event

  def event_type # legacy
    attributes['event_type'] || (attributes['event_id'].nil? ? 'none' : event ? event.name : attributes['event_id'])
  end

  def update_stats_for_target(target)
    return unless target

    target.reading_count += 1
    target.first_reading = self unless target.first_reading
    target.last_reading = self
    target.oldest_reading_at = self.created_at if target.oldest_reading_at.nil? or target.oldest_reading_at > self.created_at
    target.newest_reading_at = self.created_at if target.newest_reading_at.nil? or target.newest_reading_at <= self.created_at
  end
end
