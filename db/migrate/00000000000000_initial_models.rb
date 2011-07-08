class InitialModels < ActiveRecord::Migration
  def self.up
    create_table :captures do |t|
      t.integer :host_id,:null => false,:default => 0
      t.string :name

      t.integer :hosts_created,:null => false,:default => 0
      t.integer :hosts_updated,:null => false,:default => 0
      t.integer :accounts_created,:null => false,:default => 0
      t.integer :accounts_updated,:null => false,:default => 0
      t.integer :devices_created,:null => false,:default => 0
      t.integer :devices_updated,:null => false,:default => 0
      t.integer :gateways_created,:null => false,:default => 0
      t.integer :gateways_updated,:null => false,:default => 0
      t.integer :events_created,:null => false,:default => 0
      t.integer :events_updated,:null => false,:default => 0

      t.integer :reading_count,:null => false,:default => 0
      t.integer :first_reading_id,:null => false,:default => 0
      t.integer :last_reading_id,:null => false,:default => 0
      t.datetime :oldest_reading_at
      t.datetime :newest_reading_at

      t.timestamps
    end

    create_table :hosts do |t|
      t.integer :capture_id,:null => false,:default => 0
      t.string :name
      t.string :timezone

      t.integer :reading_count,:null => false,:default => 0
      t.integer :first_reading_id,:null => false,:default => 0
      t.integer :last_reading_id,:null => false,:default => 0
      t.datetime :oldest_reading_at
      t.datetime :newest_reading_at

      t.timestamps
    end

    create_table :accounts do |t|
      t.integer :capture_id,:null => false,:default => 0
      t.integer :host_id,:null => false,:default => 0
      t.string :name

      t.integer :reading_count,:null => false,:default => 0
      t.integer :first_reading_id,:null => false,:default => 0
      t.integer :last_reading_id,:null => false,:default => 0
      t.datetime :oldest_reading_at
      t.datetime :newest_reading_at

      t.timestamps
    end

    create_table :devices do |t|
      t.integer :capture_id,:null => false,:default => 0
      t.integer :account_id,:null => false,:default => 0
      t.integer :gateway_id,:null => false,:default => 0
      t.string :name
      t.string :imei

      t.integer :reading_count,:null => false,:default => 0
      t.integer :first_reading_id,:null => false,:default => 0
      t.integer :last_reading_id,:null => false,:default => 0
      t.datetime :oldest_reading_at
      t.datetime :newest_reading_at

      t.timestamps
    end

    create_table :gateways do |t|
      t.integer :capture_id,:null => false,:default => 0
      t.integer :host_id,:null => false,:default => 0
      t.string :name

      t.integer :reading_count,:null => false,:default => 0
      t.integer :first_reading_id,:null => false,:default => 0
      t.integer :last_reading_id,:null => false,:default => 0
      t.datetime :oldest_reading_at
      t.datetime :newest_reading_at
      
      t.timestamps
    end

    create_table :events do |t|
      t.string :name
      t.integer :capture_id,:null => false,:default => 0
      t.integer :gateway_id,:null => false,:default => 0

      t.integer :reading_count,:null => false,:default => 0
      t.integer :first_reading_id,:null => false,:default => 0
      t.integer :last_reading_id,:null => false,:default => 0
      t.datetime :oldest_reading_at
      t.datetime :newest_reading_at

      t.timestamps
    end

    create_table :readings do |t|
      t.integer :capture_id,:null => false,:default => 0
      t.integer :host_id,:null => false,:default => 0
      t.integer :account_id,:null => false,:default => 0
      t.integer :device_id,:null => false,:default => 0
      t.integer :gateway_id,:null => false,:default => 0
      t.integer :event_id,:null => false,:default => 0
      t.decimal :latitude,:precision => 15,:scale => 10
      t.decimal :longitude,:precision => 15,:scale => 10
      t.boolean :ignition
      t.integer :speed

      t.timestamps
    end

    partitions = []
    current_date = Time.parse('2007-01-01 UTC')
    end_date = Time.parse('2012-01-01 UTC')
    while current_date <= end_date do
      partitions << current_date.strftime("partition y%Ym%m values less than (to_days('%Y-%m-%d'))")
      current_date = current_date.advance(:months => 1)
    end

    execute %(alter table readings drop primary key, modify created_at datetime not null default '0000-00-00 00:00:00', add primary key(id,created_at))
    execute %(alter table readings partition by range (to_days(created_at)) (
      #{partitions.join(",\n      ")}))

  end

  def self.down
    drop_table :captures
    drop_table :gateways
    drop_table :events
    drop_table :hosts
    drop_table :accounts
    drop_table :devices
    drop_table :readings
  end
end
