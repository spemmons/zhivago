module ReadingStats
  def self.included(klass)
    klass.has_many :readings

    klass.belongs_to :first_reading,:class_name => Reading.to_s
    klass.belongs_to :last_reading,:class_name => Reading.to_s
  end
  
  def reset_reading_stats
    self.reading_count = 0
    self.first_reading = nil
    self.last_reading = nil
    self.oldest_reading_at = nil
    self.newest_reading_at = nil
    return unless (result = self.readings.all :select => 'count(*) total,min(id) min_id,max(id) max_id,min(created_at) min_at,max(created_at) max_at') and (summary = result[0])

    self.reading_count = summary.total
    self.first_reading_id = summary.min_id
    self.last_reading_id = summary.max_id
    self.oldest_reading_at = summary.min_at
    self.newest_reading_at = summary.max_at
    self
  ensure
    self.save!
  end
end