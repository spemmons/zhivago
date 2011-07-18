# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# Note that this schema.rb definition is the authoritative source for your
# database schema. If you need to create the application database on another
# system, you should be using db:schema:load, not running all the migrations
# from scratch. The latter is a flawed and unsustainable approach (the more migrations
# you'll amass, the slower it'll run and the greater likelihood for issues).
#
# It's strongly recommended to check this file into your version control system.

ActiveRecord::Schema.define(:version => 1) do

  create_table "accounts", :force => true do |t|
    t.integer  "capture_id",        :default => 0, :null => false
    t.integer  "host_id",           :default => 0, :null => false
    t.string   "name"
    t.integer  "reading_count",     :default => 0, :null => false
    t.integer  "first_reading_id",  :default => 0, :null => false
    t.integer  "last_reading_id",   :default => 0, :null => false
    t.datetime "oldest_reading_at"
    t.datetime "newest_reading_at"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "captures", :force => true do |t|
    t.integer  "host_id",           :default => 0, :null => false
    t.string   "name"
    t.integer  "hosts_created",     :default => 0, :null => false
    t.integer  "hosts_updated",     :default => 0, :null => false
    t.integer  "accounts_created",  :default => 0, :null => false
    t.integer  "accounts_updated",  :default => 0, :null => false
    t.integer  "devices_created",   :default => 0, :null => false
    t.integer  "devices_updated",   :default => 0, :null => false
    t.integer  "gateways_created",  :default => 0, :null => false
    t.integer  "gateways_updated",  :default => 0, :null => false
    t.integer  "events_created",    :default => 0, :null => false
    t.integer  "events_updated",    :default => 0, :null => false
    t.integer  "reading_count",     :default => 0, :null => false
    t.integer  "first_reading_id",  :default => 0, :null => false
    t.integer  "last_reading_id",   :default => 0, :null => false
    t.datetime "oldest_reading_at"
    t.datetime "newest_reading_at"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "devices", :force => true do |t|
    t.integer  "capture_id",        :default => 0, :null => false
    t.integer  "account_id",        :default => 0, :null => false
    t.integer  "gateway_id",        :default => 0, :null => false
    t.string   "name"
    t.string   "imei"
    t.integer  "reading_count",     :default => 0, :null => false
    t.integer  "first_reading_id",  :default => 0, :null => false
    t.integer  "last_reading_id",   :default => 0, :null => false
    t.datetime "oldest_reading_at"
    t.datetime "newest_reading_at"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "events", :force => true do |t|
    t.string   "name"
    t.integer  "capture_id",        :default => 0, :null => false
    t.integer  "gateway_id",        :default => 0, :null => false
    t.integer  "reading_count",     :default => 0, :null => false
    t.integer  "first_reading_id",  :default => 0, :null => false
    t.integer  "last_reading_id",   :default => 0, :null => false
    t.datetime "oldest_reading_at"
    t.datetime "newest_reading_at"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "gateways", :force => true do |t|
    t.integer  "capture_id",        :default => 0, :null => false
    t.integer  "host_id",           :default => 0, :null => false
    t.string   "name"
    t.integer  "reading_count",     :default => 0, :null => false
    t.integer  "first_reading_id",  :default => 0, :null => false
    t.integer  "last_reading_id",   :default => 0, :null => false
    t.datetime "oldest_reading_at"
    t.datetime "newest_reading_at"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "hosts", :force => true do |t|
    t.integer  "capture_id",        :default => 0, :null => false
    t.string   "name"
    t.integer  "reading_count",     :default => 0, :null => false
    t.integer  "first_reading_id",  :default => 0, :null => false
    t.integer  "last_reading_id",   :default => 0, :null => false
    t.datetime "oldest_reading_at"
    t.datetime "newest_reading_at"
    t.datetime "created_at"
    t.datetime "updated_at"
    t.string   "timezone"
  end

  create_table "periodic_stats", :force => true do |t|
    t.integer  "gateway_id",        :default => 0, :null => false
    t.datetime "starting_at",                      :null => false
    t.string   "host_name"
    t.string   "gateway_name"
    t.integer  "devices_available", :default => 0, :null => false
    t.integer  "devices_reported",  :default => 0, :null => false
    t.integer  "readings_sent",     :default => 0, :null => false
  end

  create_table "readings", :id => false, :force => true do |t|
    t.integer  "id",                                                        :null => false
    t.integer  "capture_id",                                 :default => 0, :null => false
    t.integer  "host_id",                                    :default => 0, :null => false
    t.integer  "account_id",                                 :default => 0, :null => false
    t.integer  "device_id",                                  :default => 0, :null => false
    t.integer  "gateway_id",                                 :default => 0, :null => false
    t.integer  "event_id",                                   :default => 0, :null => false
    t.decimal  "latitude",   :precision => 15, :scale => 10
    t.decimal  "longitude",  :precision => 15, :scale => 10
    t.boolean  "ignition"
    t.integer  "speed"
    t.datetime "created_at",                                                :null => false
    t.datetime "updated_at"
  end

  add_index "readings", ["capture_id"], :name => "index_readings_on_capture_id"
  add_index "readings", ["device_id", "created_at"], :name => "index_readings_on_device_id_and_created_at"
  add_index "readings", ["host_id", "created_at"], :name => "index_readings_on_host_id_and_created_at"

end
