class InitialIndices < ActiveRecord::Migration
  def self.up
    add_index :readings,:capture_id
    add_index :readings,[:host_id,:created_at]
    add_index :readings,[:device_id,:created_at]
  end

  def self.down
    remove_index :readings,:capture_id
    remove_index :readings,[:created_at,:host_id]
    remove_index :readings,[:created_at,:device_id]
  end
end
