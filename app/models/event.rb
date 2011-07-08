class Event < ActiveRecord::Base
  belongs_to :capture
  belongs_to :gateway

  include ReadingStats
end
