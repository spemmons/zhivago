class Host < ActiveRecord::Base
  belongs_to :capture

  has_many :captures,:dependent => :destroy
  has_many :accounts
  has_many :devices,:through => :accounts
  has_many :gateways
  has_many :events,:through => :gateways

  include ReadingStats

  def recalc_reading_stats
    self.reset_reading_stats
    self.captures.each{|entry| entry.reset_reading_stats}
#    self.accounts.each{|entry| entry.reset_reading_stats}
    self.devices.each{|entry| entry.reset_reading_stats}
#    self.gateways.each{|entry| entry.reset_reading_stats}
#    self.events.each{|entry| entry.reset_reading_stats}
  end

end
