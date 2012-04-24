module Entropy

  def calculate_divergence(ps,qs)
    ps.length.times.inject(0.0){|sum,index| p,q = ps[index],qs[index]; sum.nil? || (p != 0.0 && q == 0.0) ? nil : p == 0.0 ? sum : sum + p*Math.log2(p/q)} if ps.length == qs.length
  end

  def calculate_basic_entropy(*ps)
    raise "total probability does not equal 1.0 -- #{ps}" unless check_effective_equality(ps.inject(0.0){|sum,p| sum + p},1.0)
    ps.inject(0.0){|sum,p| p == 0.0 ? sum : sum - p*Math.log2(p)}
  end

  def check_effective_equality(a,b)
    (a - b).abs < 0.0001
  end

end