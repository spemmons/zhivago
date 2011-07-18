require 'test_helper'

class PeriodTest < ActiveSupport::TestCase
  test 'nearest hour handling' do
    assert_equal Time.gm(2000,1,2,3),Period.nearest_hour(Time.gm(2000,1,2,3,4))
    assert_equal [Time.gm(2000,1,2,3)],Period.hour_range(Time.gm(2000,1,2,3,4),Time.gm(2000,1,2,3,40))
    assert_equal [Time.gm(2000,1,2,3),Time.gm(2000,1,2,4)],Period.hour_range(Time.gm(2000,1,2,3,4),Time.gm(2000,1,2,4,40))
  end
end
