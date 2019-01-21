package profile;

import org.apache.spark.util.AccumulatorV2;

import java.util.HashMap;
import java.util.Map;

/**
 * @author chimney
 */
public class DemoAccumulator extends AccumulatorV2 {

	private Map<String, Integer> map = new HashMap<>();

	@Override
	public boolean isZero() {
		return this.map.isEmpty();
	}

	@Override
	public AccumulatorV2 copy() {
		DemoAccumulator accumulator = new DemoAccumulator();
		accumulator.map = this.map;
		return accumulator;
	}

	@Override
	public void reset() {
		map = new HashMap<>();
	}

	@Override
	public void add(Object o) {
		String key = o == null ? "null" : o.toString();
		if (key.length() > 10) {
			key = key.substring(0, 10);
		}
		if (map.containsKey(key)) {
			map.put(key, map.get(key) + 1);
		} else {
			map.put(key, 1);
		}
	}

	@Override
	public void merge(AccumulatorV2 accumulatorV2) {
		Map<String, Integer> toMerge = (Map<String, Integer>) accumulatorV2.value();
		if (map.size() == 0) {
			map = toMerge;
		} else {
			for (Map.Entry<String, Integer> entry : toMerge.entrySet()) {
				if (map.containsKey(entry.getKey())) {
					map.put(entry.getKey(), map.get(entry.getKey()) + entry.getValue());
				} else {
					map.put(entry.getKey(), entry.getValue());
				}
			}
		}
	}

	@Override
	public Object value() {
		return this.map;
	}

}
