class SpacingStudy
  include Entropy

  def initialize(outfile = STDOUT)
    @outfile  = outfile
  end

  def linear_region_boundaries(total_width,sensor_width,sensor_count,sensor_spacing)
    raise 'sensor spacing must be <= sensor width' if sensor_spacing > sensor_width
    raise 'sensor spacing makes overall sensor span larger than total width' if sensor_width + (sensor_count - 1) * sensor_spacing > total_width

    position,enter_queue,exit_queue = 0.0,[],[]
    sensor_count.times do
      enter_queue.push position
      exit_queue.push position + sensor_width
      position += sensor_spacing
    end

    boundaries = []
    while enter_queue.any? or exit_queue.any?
      best_enter = enter_queue.first
      best_exit = exit_queue.first
      if best_enter and best_exit and best_enter < best_exit
        best_exit = nil
      else
        best_enter = nil
      end
      boundaries.push enter_queue.shift if best_enter
      boundaries.push exit_queue.shift if best_exit
    end
    boundaries
  end

  def linear_region_widths(total_width,sensor_width,sensor_count,sensor_spacing)
    build_region_widths(total_width,linear_region_boundaries(total_width,sensor_width,sensor_count,sensor_spacing))
  end

  def build_region_widths(total_width,boundaries)
    sensor_coverage,last_boundary,widths = 0.0,nil,[]
    boundaries.uniq.each_with_index do |boundary,index|
      sensor_coverage += (widths[index] = boundary - last_boundary) if last_boundary
      last_boundary = boundary
    end
    widths[0] = total_width - sensor_coverage
    widths
  end

  def random_region_widths(total_width,sensor_width,sensor_count)
    enter_queue = sensor_count.times.collect{(total_width - sensor_width) * rand}.sort

    widths,exit_queue,last_position = [0],[],nil
    while enter_queue.any? or exit_queue.any?
      best_enter = enter_queue.first
      best_exit = exit_queue.first
      if last_position.nil?
        raise 'invalid condition - no enter exists when no position set' unless best_enter
        raise 'invalid condition - exit exists when no position set' if best_exit
        enter_queue.shift
        last_position = best_enter
        exit_queue.push best_enter + sensor_width
      else
        raise 'invalid condition - no exit exists when position set' unless best_exit
        if best_enter and best_enter < best_exit
          widths.push best_enter - last_position
          enter_queue.shift
          last_position = best_enter
          exit_queue.push best_enter + sensor_width
        else
          widths.push best_exit - last_position
          exit_queue.shift
          last_position = exit_queue.any? ? best_exit : nil
        end
      end
    end

    widths[0] = total_width - widths.inject(0.0){|sum,value| sum + value}
    widths
  end

  def study_entropy_by_spacing_and_coverage(total_width,sensor_width,steps)
    raise 'at least 2 sensors must be possible' unless (max_sensor_count = total_width / sensor_width) >= 2

    put_headings
    max_spacing = [sensor_width.to_f,(total_width - sensor_width).to_f / (max_sensor_count - 1)].min
    2.upto(max_sensor_count) do |sensor_count|
      study_sensor_count_by_spacing_and_coverage(sensor_count,max_spacing,total_width,sensor_width,steps)
    end
  end

  # File.open('../100-sensors-evenly-spaced.csv','w'){|file| SpacingStudy.new(file).study_sensor_count_by_spacing_and_coverage(100,1.0,100.0,1.0,1000)}
  # File.open('../150-sensors-evenly-spaced.csv','w'){|file| SpacingStudy.new(file).study_sensor_count_by_spacing_and_coverage(150,1.0,100.0,1.0,1000)}
  # File.open('../200-sensors-evenly-spaced.csv','w'){|file| SpacingStudy.new(file).study_sensor_count_by_spacing_and_coverage(200,1.0,100.0,1.0,1000)}
  def study_sensor_count_by_spacing_and_coverage(sensor_count,max_spacing,total_width,sensor_width,steps)
    put_headings
    0.step(max_spacing,max_spacing / steps) do |sensor_spacing|
      study_sensor_count_with_even_spacing(sensor_count,sensor_spacing,total_width,sensor_width)
    end
  end

  def study_sensor_count_with_even_spacing(sensor_count,sensor_spacing,total_width,sensor_width)
    put_headings
    put_tuple(sensor_count,sensor_spacing,total_width,linear_region_widths(total_width,sensor_width,sensor_count,sensor_spacing))
  end

  # File.open('../100-sensors-random.csv','w'){|file| study = SpacingStudy.new(file); 100.times{study.study_sensor_count_with_random_spacing(100,100.0,1.0)}}
  # File.open('../150-sensors-random.csv','w'){|file| study = SpacingStudy.new(file); 100.times{study.study_sensor_count_with_random_spacing(150,100.0,1.0)}}
  # File.open('../200-sensors-random.csv','w'){|file| study = SpacingStudy.new(file); 100.times{study.study_sensor_count_with_random_spacing(200,100.0,1.0)}}
  def study_sensor_count_with_random_spacing(sensor_count,total_width,sensor_width)
    put_headings
    put_tuple(sensor_count,-1,total_width,random_region_widths(total_width,sensor_width,sensor_count))
  end

  def put_headings
    @outfile.puts 'sensors,spacing,coverage,total_regions,total_entropy,sensor_regions,sensor_entropy,width_min,width_max,width_ave,width_std,accuracy,success' unless @put_headings
    @put_headings = true
  end

  def put_tuple(sensor_count,sensor_spacing,total_width,widths)
    all_regions = build_region_probabilities(total_width,widths,true)
    sensor_regions = build_region_probabilities(total_width,widths,false)
#      puts "WARNING: remaining width should be 0: #{regions.first}" unless width.first == 0
    widths.shift
    sensor_coverage = 1 - all_regions.first
    accuracy = widths.length / (sensor_coverage * total_width)
    success = accuracy * sensor_coverage
    @outfile.puts format('%d,%0.4f,%0.6f,%d,%0.6f,%d,%0.6f,%0.4f,%0.4f,%0.4f,%0.6f,%0.6f,%0.6f',sensor_count,sensor_spacing,sensor_coverage,all_regions.length,calculate_basic_entropy(*all_regions),sensor_regions.length,calculate_basic_entropy(*sensor_regions),*compute_min_max_ave_std(*widths),accuracy,success)
  end

  def build_region_probabilities(total_width,widths,include_remainder = true)
    widths = widths.dup
    total_width -= widths.shift unless include_remainder
    total_width <= 0 ? [0] : widths.collect{|width| width.to_f / total_width}
  end

  def study_entropy_by_sensor_count(total_width,sensor_width,min_spacing)
    raise 'min spacing must be > 0' unless min_spacing > 0.0
    raise 'at least 1 sensor must be possible' unless (sensor_count = total_width / sensor_width) > 0

    @outfile.puts 'count,spacing,regions,entropy,width_min,width_max,width_ave,width_std'
    while (sensor_spacing = (total_width - sensor_width).to_f / (sensor_count - 1)) >= min_spacing
      widths = linear_region_widths(total_width,sensor_width,sensor_count,sensor_spacing)
#      puts "WARNING: remaining width should be 0: #{regions.first}" unless width.first == 0
      widths.shift
      widths = remove_zeros(*widths)
      regions = widths.collect{|width| width.to_f / total_width}
      @outfile.puts format('%d,%0.4f,%d,%0.6f,%0.6f,%0.6f,%0.6f,%0.6f',sensor_count,sensor_spacing,regions.length,calculate_basic_entropy(*regions),*compute_min_max_ave_std(*widths))
      sensor_count += 1
    end
  end

  def remove_zeros(*values)
    values.inject([]){|result,value| result << value if value > Float::EPSILON; result}
  end

  def compute_min_max_ave_std(*values)
    raise 'must be 1 value' unless values.length > 0

    value_min,value_max,value_total = Float::MAX,Float::MIN,0
    values.each do |value|
      value_min = value if value < value_min
      value_max = value if value > value_max
      value_total += value
    end

    value_ave = value_total.to_f / values.length
#    value_std = Math.sqrt(values.inject(0.0){|sum,value| sum + (value_ave - value)**2} / values.length)
    total_width = values.inject(0.0){|sum,value| sum + value}
    value_std = Math.sqrt(values.inject(0.0){|sum,value| sum + (value)**2 * value/total_width})

    [value_min,value_max,value_ave,value_std]
  end

end