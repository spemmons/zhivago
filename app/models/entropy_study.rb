class EntropyStudy

  include Entropy

  DEVICE_OFFSET =   0
  READING_OFFSET =  1
  
  P_HD =                      "P(HD)"
  P_GD =                      "P(GD)"
  P_HD_AND_GD =               "P(HD,GD)"
  P_HM =                      "P(HM)"
  P_GM =                      "P(GM)"
  P_HM_AND_GM =               "P(HM,GM)"

  P_H =                       [P_HD,P_HM]
  P_G =                       [P_GD,P_GM]
  P_H_AND_G =                 [P_HD_AND_GD,P_HM_AND_GM]
  
  S_HD =                      "S(HD)"
  S_GD =                      "S(GD)"
  S_HD_AND_GD =               "S(HD,GD)"
  S_HD_GIVEN_GD =             "S(HD|GD)"
  S_GD_GIVEN_HD =             "S(GD|HD)"
  I_HD_GD =                   "I(HD;GD)"
  S_HM =                      "S(HM)"
  S_GM =                      "S(GM)"
  S_HM_AND_GM =               "S(HM,GM)"
  S_HM_GIVEN_GM =             "S(HM|GM)"
  S_GM_GIVEN_HM =             "S(GM|HM)"
  I_HM_GM =                   "I(HM;GM)"
  D_HD_WRT_HM =               "D(P(HD)||P(HM))"
  D_HM_WRT_HD =               "D(P(HM)||P(HD))"
  D_GD_WRT_GM =               "D(P(GD)||P(GM))"
  D_GM_WRT_GD =               "D(P(GM)||P(GD))"
  D_HD_AND_GD_WRT_HM_AND_GM = "D(P(HD,GD)||P(HM,GM))"
  D_HM_AND_GM_WRT_HD_AND_GD = "D(P(HM,GM)||P(HD,GD))"
  D_HD_WRT_PREVIOUS =         "D(P(HD)||P'(HD))"
  D_HM_WRT_PREVIOUS =         "D(P(HM)||P'(HM))"
  D_GD_WRT_PREVIOUS =         "D(P(GD)||P'(GD))"
  D_GM_WRT_PREVIOUS =         "D(P(GM)||P'(GM))"
  D_HD_AND_GD_WRT_PREVIOUS =  "D(P(HD,GD)||P'(HD,GD))"
  D_HM_AND_GM_WRT_PREVIOUS =  "D(P(HM,GM)||P'(HM,GM))"
  D_HD_WRT_OVERALL =          "D(P(HD)||Pt(HD))"
  D_HM_WRT_OVERALL =          "D(P(HM)||Pt(HM))"
  D_GD_WRT_OVERALL =          "D(P(GD)||Pt(GD))"
  D_GM_WRT_OVERALL =          "D(P(GM)||Pt(GM))"
  D_HD_AND_GD_WRT_OVERALL =   "D(P(HD,GD)||Pt(HD,GD))"
  D_HM_AND_GM_WRT_OVERALL =   "D(P(HM,GM)||Pt(HM,GM))"

  S_H =               [S_HD,S_HM]
  S_G =               [S_GD,S_GM]
  S_H_AND_G =         [S_HD_AND_GD,S_HM_AND_GM]
  S_H_GIVEN_G =       [S_HD_GIVEN_GD,S_HM_GIVEN_GM]
  S_G_GIVEN_H =       [S_GD_GIVEN_HD,S_GM_GIVEN_HM]
  I_H_G =             [I_HD_GD,I_HM_GM]
  D_H_WRT_H =         [D_HD_WRT_HM,D_HM_WRT_HD]
  D_G_WRT_G =         [D_GD_WRT_GM,D_GM_WRT_GD]
  D_HG_WRT_HG =       [D_HD_AND_GD_WRT_HM_AND_GM,D_HM_AND_GM_WRT_HD_AND_GD]

  ALL_ENTROPY_LABELS = [
    S_HD,
    #S_GD,
    #S_HD_AND_GD,
    #S_HD_GIVEN_GD,
    #S_GD_GIVEN_HD,
    #I_HD_GD,
    #S_HM,
    #S_GM,
    #S_HM_AND_GM,
    #S_HM_GIVEN_GM,
    #S_GM_GIVEN_HM,
    #I_HM_GM,
    #D_HD_WRT_HM,
    #D_HM_WRT_HD,
    #D_GD_WRT_GM,
    #D_GM_WRT_GD,
    #D_HD_AND_GD_WRT_HM_AND_GM,
    #D_HM_AND_GM_WRT_HD_AND_GD,
    D_HD_WRT_PREVIOUS,
    #D_HM_WRT_PREVIOUS,
    #D_GD_WRT_PREVIOUS,
    #D_GM_WRT_PREVIOUS,
    #D_HD_AND_GD_WRT_PREVIOUS,
    #D_HM_AND_GM_WRT_PREVIOUS,
    D_HD_WRT_OVERALL,
    #D_HM_WRT_OVERALL,
    #D_GD_WRT_OVERALL,
    #D_GM_WRT_OVERALL,
    #D_HD_AND_GD_WRT_OVERALL,
    #D_HM_AND_GM_WRT_OVERALL,
  ]

  QUEUE_LENGTH = 7

  PERIODIC_STAT_QUERY = "select from_days(to_days(starting_at)) period,host_name,gateway_name,max(devices_available) device_count,sum(readings_sent) readings_count from periodic_stats where starting_at < '2011-07-01' group by host_name,gateway_name,to_days(starting_at) order by to_days(starting_at)"
#  PERIODIC_STAT_QUERY = "select from_days(to_days(starting_at)) period,host_name,gateway_name,max(devices_available) device_count,sum(readings_sent) readings_count from periodic_stats where starting_at between '2009-09-23' and '2009-09-26' group by host_name,gateway_name,to_days(starting_at) order by to_days(starting_at)"

  def self.daily_study(output = STDOUT)
    output.puts "period\tdevices\treadings\thosts\tgateways\t#{ALL_ENTROPY_LABELS.join("\t")}"

    last_study,current_study,overall_study,study_queue = nil,nil,new(output),Queue.new
    PeriodicStat.connection.select_rows(PERIODIC_STAT_QUERY).each do |row|
      if current_study.nil?
        current_study = new(output)
      elsif row[0] != current_study.period
        current_study.harvest_entropy(ALL_ENTROPY_LABELS,last_study,overall_study)
        last_study,current_study = current_study,new(output)
        study_queue.enq last_study
        overall_study.exclude_frequencies(study_queue.deq) if study_queue.length > QUEUE_LENGTH
      end

      overall_study.collect_daily_frequencies(nil,*row[1..-1])
      current_study.collect_daily_frequencies(*row)
    end
    
    current_study.harvest_entropy(ALL_ENTROPY_LABELS,last_study,overall_study) if current_study
    overall_study.harvest_entropy(ALL_ENTROPY_LABELS,nil,nil)
  end

  attr_reader :period,:device_total,:reading_total,:host_names,:gateway_names,:frequencies,:probabilities,:calculations

  def initialize(output = STDOUT)
    @output = output
    reset_state
  end

  def reset_state
    @period,@totals,@host_names,@gateway_names,@frequencies,@probabilities,@calculations = nil,[0,0],{},{},{},{},{}
  end

  def collect_daily_frequencies(period,host_name,gateway_name,device_count,reading_count)
    raise "period #{period} not the same as #{@period}" if @period and period != @period

    @period = period
    @host_names[host_name] ||= true
    @gateway_names[gateway_name] ||= true

    @totals[DEVICE_OFFSET] += device_count
    @totals[READING_OFFSET] += reading_count
    counts = (@frequencies[host_name] ||= {})[gateway_name] ||= [0,0]
    counts[DEVICE_OFFSET] += device_count
    counts[READING_OFFSET] += reading_count
  end

  def harvest_entropy(labels,last_study,overall_study)
    harvest_entropy_by_offset(DEVICE_OFFSET)
    harvest_entropy_by_offset(READING_OFFSET)
    harvest_divergence_pair(D_H_WRT_H,@probabilities[P_HD],@probabilities[P_HM])
    harvest_divergence_pair(D_G_WRT_G,@probabilities[P_GD],@probabilities[P_GM])
    harvest_divergence_pair(D_HG_WRT_HG,@probabilities[P_HD_AND_GD],@probabilities[P_HM_AND_GM])
    
    if last_study and overall_study
      
      if collect_probabilities(DEVICE_OFFSET,overall_study.host_names,overall_study.gateway_names)
        if last_study.collect_probabilities(DEVICE_OFFSET,overall_study.host_names,overall_study.gateway_names)
          capture_calculation(D_HD_WRT_PREVIOUS,calculate_divergence(@probabilities[P_HD],last_study.probabilities[P_HD]))
          capture_calculation(D_GD_WRT_PREVIOUS,calculate_divergence(@probabilities[P_GD],last_study.probabilities[P_GD]))
          capture_calculation(D_HD_AND_GD_WRT_PREVIOUS,calculate_divergence(@probabilities[P_HD_AND_GD],last_study.probabilities[P_HD_AND_GD]))
        end

        if overall_study.collect_probabilities(DEVICE_OFFSET)
          capture_calculation(D_HD_WRT_OVERALL,calculate_divergence(@probabilities[P_HD],overall_study.probabilities[P_HD]))
          capture_calculation(D_GD_WRT_OVERALL,calculate_divergence(@probabilities[P_GD],overall_study.probabilities[P_GD]))
          capture_calculation(D_HD_AND_GD_WRT_OVERALL,calculate_divergence(@probabilities[P_HD_AND_GD],overall_study.probabilities[P_HD_AND_GD]))
        end
      end

      if collect_probabilities(READING_OFFSET,overall_study.host_names,overall_study.gateway_names)
        if last_study.collect_probabilities(READING_OFFSET,overall_study.host_names,overall_study.gateway_names)
          capture_calculation(D_HM_WRT_PREVIOUS,calculate_divergence(@probabilities[P_HM],last_study.probabilities[P_HM]))
          capture_calculation(D_GM_WRT_PREVIOUS,calculate_divergence(@probabilities[P_GM],last_study.probabilities[P_GM]))
          capture_calculation(D_HM_AND_GM_WRT_PREVIOUS,calculate_divergence(@probabilities[P_HM_AND_GM],last_study.probabilities[P_HM_AND_GM]))
        end

        if overall_study.collect_probabilities(READING_OFFSET)
          capture_calculation(D_HM_WRT_OVERALL,calculate_divergence(@probabilities[P_HM],overall_study.probabilities[P_HM]))
          capture_calculation(D_GM_WRT_OVERALL,calculate_divergence(@probabilities[P_GM],overall_study.probabilities[P_GM]))
          capture_calculation(D_HM_AND_GM_WRT_OVERALL,calculate_divergence(@probabilities[P_HM_AND_GM],overall_study.probabilities[P_HM_AND_GM]))
        end
      end
    end

    @output.print format("%s\t%d\t%d\t%d\t%d",(@period && @period.strftime('%Y-%m-%d')),*@totals,@host_names.length,@gateway_names.length)
    output_entropic_values(*labels)
    @output.puts
  end

  def harvest_entropy_by_offset(offset)
    return unless collect_probabilities(offset)

    capture_calculation(S_H[offset],        host_entropy = calculate_basic_entropy(*@probabilities[P_H[offset]]))
    capture_calculation(S_G[offset],        gateway_entropy = calculate_basic_entropy(*@probabilities[P_G[offset]]))
    capture_calculation(S_H_AND_G[offset],  host_and_gateway_entropy = calculate_basic_entropy(*@probabilities[P_H_AND_G[offset]]))
    capture_calculation(S_H_GIVEN_G[offset],host_and_gateway_entropy - gateway_entropy)
    capture_calculation(S_G_GIVEN_H[offset],host_and_gateway_entropy - host_entropy)
    capture_calculation(I_H_G[offset],      host_entropy + gateway_entropy - host_and_gateway_entropy)
  end

  def harvest_divergence_pair(labels,device_ps,reading_ps)
    capture_calculation(labels[0],calculate_divergence(device_ps,reading_ps))
    capture_calculation(labels[1],calculate_divergence(reading_ps,device_ps))
  end

  def output_entropic_values(*labels)
    labels.each{|label| value = @calculations[label]; @output.print value ? format("\t%0.04f" % value) : "\t"}
  end

  def collect_probabilities(offset,host_scope = @host_names,gateway_scope = @gateway_names)
    return unless (total = @totals[offset]) > 0

    @probabilities[P_H[offset]] = ps = []
    host_scope.keys.sort.each{|host_name| gateway_hash = @frequencies[host_name] || {}; ps << (gateway_hash.values.inject(0.0){|sum,counts| sum + counts[offset]}).to_f / total.to_f}

    @probabilities[P_G[offset]] = ps = []
    gateway_scope.keys.sort.each{|gateway_name| ps << @frequencies.values.inject(0.0){|sum,gateway_hash| gateway_hash[gateway_name] ? sum + gateway_hash[gateway_name][offset] : sum}.to_f / total.to_f}

    @probabilities[P_H_AND_G[offset]] = ps = []
    host_scope.keys.sort.each{|host_name| gateway_hash = @frequencies[host_name] || {}; gateway_scope.keys.each{|gateway_name| ps << ((counts = gateway_hash[gateway_name]) ? counts[offset].to_f / total.to_f : 0.0)}}

    total
  end

  def exclude_frequencies(other_study)

  end

  def capture_calculation(label,value)
    raise "#{label} was #{@calculations[label]} but now is #{value}" if @calculations[label] and not check_effective_equality(@calculations[label],value)
    @calculations[label] = value
  end

end