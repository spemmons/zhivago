class Gateway < ActiveRecord::Base
  belongs_to :capture
  belongs_to :host

  has_many :devices
  has_many :events
  has_many :periodic_stats

  include ReadingStats
end
