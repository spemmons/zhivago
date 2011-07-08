class Device < ActiveRecord::Base
  belongs_to :capture
  belongs_to :account
  belongs_to :gateway

  include ReadingStats

  def gateway_name # legacy
    attributes['gateway_name'] || (attributes['gateway_id'] && gateway.name) || 'none'
  end
end
