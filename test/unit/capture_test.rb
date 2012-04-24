require 'test_helper'

class CaptureTest < ActiveSupport::TestCase
  test 'new host created and existing host updated' do
    existing_host = Host.create!(:name => 'a')

    assert_no_difference 'Host.count' do
      capture = Capture.for_host('a')
      assert_equal existing_host,capture.host
      assert_equal 0,capture.hosts_created
      assert_equal 1,capture.hosts_updated
    end

    assert_difference 'Host.count' do
      capture = Capture.for_host('b')
      assert_equal 1,capture.hosts_created
      assert_equal 0,capture.hosts_updated
    end
  end

  test 'handle account caching' do
    capture = Capture.for_host('test-host')
    account = capture.host.accounts.create!(:name => 'test-account1')

    assert_no_difference 'Account.count' do
      account1 = capture.note_account(account.name)
      assert_equal account,account1
      assert_equal account1,capture.lookup_account(1)
      assert_nil account1.capture
    end

    assert_difference 'Account.count' do
      account2 = capture.note_account('test-account2')
      assert_equal account2,capture.lookup_account(2)
      assert_equal capture,account2.capture
    end

    assert_difference 'Account.count' do
      account3 = capture.lookup_account(3)
      assert_not_nil account3.name == 'none'
      assert_equal account3,capture.lookup_account(3)
      assert_equal capture,account3.capture
    end

    assert_equal 2,capture.accounts_created
    assert_equal 1,capture.accounts_updated
  end

  test 'handle gateway caching' do
    capture = Capture.for_host('test-host')
    gateway = capture.host.gateways.create!(:name => 'test-gateway1')

    assert_no_difference 'Gateway.count' do
      gateway1 = capture.note_gateway(gateway.name)
      assert_equal gateway,gateway1
      assert_equal gateway1,capture.lookup_gateway(1)
    end

    assert_difference 'Gateway.count' do
      gateway2 = capture.note_gateway('test-gateway2')
      assert_equal gateway2,capture.lookup_gateway(2)
    end

    assert_difference 'Gateway.count' do
      gateway3 = capture.lookup_gateway(3)
      assert_not_nil gateway3.name == 'none'
      assert_equal gateway3,capture.lookup_gateway(3)
    end

    assert_equal 2,capture.gateways_created
    assert_equal 1,capture.gateways_updated
  end

  test 'handle event caching' do
    capture = Capture.for_host('test-host')
    gateway = capture.note_gateway('test-gateway')
    event = gateway.events.create!(:name => 'test-event1')

    assert_no_difference 'Event.count' do
      event1 = capture.note_event(event.name,1)
      assert_equal event,event1
      assert_equal event1,capture.lookup_event(1)
    end

    assert_difference 'Event.count' do
      event2 = capture.note_event('test-event2',0)
      assert_equal event2,capture.lookup_event(2)
      assert_not_nil event2.gateway.name == 'none'
    end

    assert_difference 'Event.count' do
      event3 = capture.lookup_event(3)
      assert_not_nil event3.name == 'none'
      assert_not_nil event3.gateway.name == 'none'
      assert_equal event3,capture.lookup_event(3)
    end

    assert_equal 2,capture.events_created
    assert_equal 1,capture.events_updated
  end

  test 'matching accounts across hosts' do
    assert_difference 'Account.count',2 do
      capture1 = Capture.for_host('test-host1')
      account1 = capture1.note_account('test-account')
      capture2 = Capture.for_host('test-host2')
      account2 = capture2.note_account('test-account')
      assert_not_equal account1,account2
    end
  end

  test 'matching gateways across hosts' do
    assert_difference 'Gateway.count',2 do
      capture1 = Capture.for_host('test-host1')
      gateway1 = capture1.note_gateway('test-gateway')
      capture2 = Capture.for_host('test-host2')
      gateway2 = capture2.note_gateway('test-gateway')
      assert_not_equal gateway1,gateway2
    end
  end

  test 'matching events across gateways' do
    assert_difference 'Event.count',2 do
      capture = Capture.for_host('test-host')
      gateway1 = capture.note_gateway('test-gateway1')
      gateway2 = capture.note_gateway('test-gateway2')
      event1 = capture.note_event('test-event',1)
      event2 = capture.note_event('test-event',2)
      assert_not_equal event1,event2
      assert_equal gateway1,event1.gateway
      assert_equal gateway2,event2.gateway
    end
  end

  test 'fix event snafu' do
    capture = Capture.for_host('test-host')
    gateway1 = capture.note_gateway('test-gateway1')
    event1 = capture.note_event('test-event',1)
    gateway2 = capture.note_gateway('test-gateway2')
    device = capture.note_device('test-device','test-imei',1,2)
    event2 = capture.fix_event_snafu(device,1)
    event3 = capture.fix_event_snafu(device,1)
    assert_not_equal event1,event2
    assert_equal event2,event3
    assert_equal event1.name,event2.name
    assert_equal gateway1,event1.gateway
    assert_equal gateway2,event2.gateway
  end

  test 'handle device caching' do
    capture = Capture.for_host('test-host')
    gateway = capture.note_gateway('test-gateway')
    account = capture.note_account('test-account')
    device = gateway.devices.create!(:name => 'test-deviceX',:imei => 'test-imei1',:account_id => account.id)
    assert_equal account,device.account
    assert_equal gateway,device.gateway

    assert_no_difference 'Device.count' do
      device1 = capture.note_device('no match',device.imei,1,1)
      assert_equal device,device1
      assert_equal device1,capture.lookup_device(1)
    end

    assert_difference 'Device.count' do
      device2 = capture.note_device('test-device2','test-imei2',1,1)
      assert_equal device2,capture.lookup_device(2)
      assert_equal account,device2.account
      assert_equal gateway,device2.gateway
    end

    assert_difference 'Device.count' do
      device3 = capture.lookup_device(3)
      assert_not_nil device3.name == 'none'
      assert_not_nil device3.account.name == 'none'
      assert_not_nil device3.gateway.name == 'none'
      assert_equal device3,capture.lookup_device(3)
    end

    assert_equal 2,capture.devices_created
    assert_equal 1,capture.devices_updated
  end

  test 'handle reading creation' do
    capture = Capture.for_host('test-host')
    gateway = capture.note_gateway('test-gateway')
    event = capture.note_event('test-event',1)
    account = capture.note_account('test-account')
    device = capture.note_device(nil,'test-imei',1,1)

    stats_array_empty = [0,nil,nil,nil,nil]
    check_reading_stats(capture,*stats_array_empty)
    check_reading_stats(capture.host,*stats_array_empty)
    check_reading_stats(account,*stats_array_empty)
    check_reading_stats(device,*stats_array_empty)
    check_reading_stats(gateway,*stats_array_empty)
    check_reading_stats(event,*stats_array_empty)

    reading1,reading2,time1,time2 = nil,nil,Time.gm(2011,2,1),Time.gm(2011,1,1)

    assert_difference 'Reading.count' do
      capture.create_reading(1,1,nil,nil,nil,nil,time1)
      first_imported_reading_id,last_imported_reading_id = capture.load_imported_readings
      assert_equal first_imported_reading_id,last_imported_reading_id
      reading1 = Reading.find(first_imported_reading_id)

      assert_equal capture,reading1.capture
      assert_equal capture.host,reading1.host
      assert_equal account,reading1.account
      assert_equal device,reading1.device
      assert_equal gateway,reading1.gateway
      assert_equal event,reading1.event
      assert_nil reading1.latitude
      assert_nil reading1.longitude
      assert_nil reading1.ignition
      assert_nil reading1.speed
      assert_equal time1,reading1.created_at
    end

    stats_array_first = [1,reading1,reading1,time1,time1]
    check_reading_stats(capture,*stats_array_first)
    check_reading_stats(capture.host,*stats_array_first)
#    check_reading_stats(account,*stats_array_first)
    check_reading_stats(device,*stats_array_first)
#    check_reading_stats(gateway,*stats_array_first)
#    check_reading_stats(event,*stats_array_first)

    assert_difference 'Reading.count' do
      capture.create_reading(2,2,10,20,1,30,time2)
      first_imported_reading_id,last_imported_reading_id = capture.load_imported_readings
      assert_equal first_imported_reading_id,last_imported_reading_id
      reading2 = Reading.find(first_imported_reading_id)
      
      assert_not_nil reading2.account.name == 'none'
      assert_not_nil reading2.device.name == 'none'
      assert_not_nil reading2.gateway.name == 'none'
      assert_not_nil reading2.event.name == 'none'
      assert_equal 10,reading2.latitude
      assert_equal 20,reading2.longitude
      assert_equal true,reading2.ignition
      assert_equal 30,reading2.speed
      assert_equal time2,reading2.created_at
    end

    stats_array_total = [2,reading1,reading2,time2,time1]
    check_reading_stats(capture,*stats_array_total)
    check_reading_stats(capture.host,*stats_array_total)

    stats_array_second = [1,reading2,reading2,time2,time2]
    check_reading_stats(capture.lookup_device(2),*stats_array_second)
#    check_reading_stats(capture.lookup_event(2),*stats_array_second)

    2.times do
      host = capture.host.reload
      assert_equal 1,host.captures.count
      assert_equal 2,host.accounts.count
      assert_equal 2,host.devices.count
      assert_equal 2,host.gateways.count
      assert_equal 2,host.events.count
      assert_equal 2,host.readings.count
      check_reading_stats(host,*stats_array_total)
      check_reading_stats(host.capture,*stats_array_total)
#      check_reading_stats(host.accounts[0],*stats_array_first)
      check_reading_stats(host.accounts[0].devices[0],*stats_array_first)
#      check_reading_stats(host.gateways[0],*stats_array_first)
#      check_reading_stats(host.gateways[0].events[0],*stats_array_first)
#      check_reading_stats(host.accounts[1],*stats_array_second)
      check_reading_stats(host.accounts[1].devices[0],*stats_array_second)
#      check_reading_stats(host.gateways[1],*stats_array_second)
#      check_reading_stats(host.gateways[1].events[0],*stats_array_second)

      capture.host.recalc_reading_stats
    end

    assert_difference 'Host.count',-1 do
      assert_difference 'Capture.count',-1 do
        assert_difference 'Account.count',-2 do
          assert_difference 'Device.count',-2 do
            assert_difference 'Gateway.count',-2 do
              assert_difference 'Event.count',-2 do
                assert_difference 'Reading.count',-2 do
                  capture.host.destroy
                end
              end
            end
          end
        end
      end
    end

  end

  test 'file import' do
    assert File.exists? 'captures/test/zhivago.csv'
    capture = Capture.import_for_host('test','PDT','test')
    assert_equal 1,capture.hosts_created
    assert_equal 1,capture.accounts_created
    assert_equal 1,capture.devices_created
    assert_equal 1,capture.gateways_created
    assert_equal 2,capture.events_created
    assert_equal 2,capture.reading_count
    assert_equal 0,capture.hosts_updated
    assert_equal 0,capture.accounts_updated
    assert_equal 0,capture.devices_updated
    assert_equal 0,capture.gateways_updated
    assert_equal 0,capture.events_updated

    reading = capture.readings.first
    assert_equal 32.6359,reading.latitude
    assert_equal -97.1757,reading.longitude
    assert_equal Time.gm(2007,8,24,11,23,3).advance(:hours => 7),reading.created_at
  end

  def check_reading_stats(target,reading_count,first_reading,last_reading,oldest_reading_at,newest_reading_at)
    assert_equal reading_count,target.reading_count
    assert_equal first_reading,target.first_reading
    assert_equal last_reading,target.last_reading
    assert_equal newest_reading_at,target.newest_reading_at
    assert_equal oldest_reading_at,target.oldest_reading_at
  end
end
