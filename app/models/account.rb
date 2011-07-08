class Account < ActiveRecord::Base
  belongs_to :capture
  belongs_to :host

  has_many :devices

  include ReadingStats

  def company # legacy
    attributes['company'] || attributes['name'] || 'none'
  end
end
