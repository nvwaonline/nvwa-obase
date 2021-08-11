package online.nvwa.obase.data;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

import javax.script.ScriptException;
import java.util.List;
import java.util.Vector;

public class CUtil {
	public static List<Cell> get(List<Cell> parent, byte[] row) {
		List<Cell> finds = new Vector<Cell>();

		for (Cell c : parent) {
			if (Bytes.equals(CellUtil.cloneRow(c), row)) {
				finds.add(c);
			}
		}

		return finds;
	}

	public static List<Cell> get(List<Cell> parent, byte[] row, byte[] family) {
		List<Cell> finds = new Vector<Cell>();

		for (Cell c : parent) {
			if (Bytes.equals(CellUtil.cloneRow(c), row)) {
				if (Bytes.equals(CellUtil.cloneFamily(c), family)) {
					finds.add(c);
				}
			}
		}

		return finds;
	}

	public static Cell get(List<Cell> parent, byte[] row, byte[] family, byte[] column) {
		for (Cell c : parent) {
			if (Bytes.equals(CellUtil.cloneRow(c), row)) {
				if (Bytes.equals(CellUtil.cloneFamily(c), family)) {
					if (Bytes.equals(CellUtil.cloneQualifier(c), column)) {
						return c;
					}
				}
			}
		}

		return null;
	}

	public static void removeRow(List<Cell> container, byte[] row) {
		List<Cell> removes = new Vector<Cell>();

		for (Cell c : container) {
			if (Bytes.equals(CellUtil.cloneRow(c), row)) {
				removes.add(c);
			}
		}

		container.removeAll(removes);
	}

	public static void removeFamily(List<Cell> container, byte[] row, byte[] family) {
		List<Cell> removes = new Vector<Cell>();

		for (Cell c : container) {
			if (Bytes.equals(CellUtil.cloneRow(c), row)) {
				if (Bytes.equals(CellUtil.cloneFamily(c), family)) {
					removes.add(c);
				}
			}
		}

		container.removeAll(removes);
	}

	public static void removeColumn(List<Cell> container, byte[] row, byte[] family, byte[] column) {
		List<Cell> removes = new Vector<Cell>();

		for (Cell c : container) {
			if (Bytes.equals(CellUtil.cloneRow(c), row)) {
				if (Bytes.equals(CellUtil.cloneFamily(c), family)) {
					if (Bytes.equals(CellUtil.cloneQualifier(c), column)) {
						removes.add(c);
					}
				}
			}
		}

		container.removeAll(removes);
	}

	@SuppressWarnings("unchecked")
	public static void addRow(List<Cell> container, byte[] row, Object value, Long ts) throws ScriptException {
		List<Cell> inputs = null;
		if (value instanceof List)
			inputs = (List<Cell>) value;
		else if (value instanceof Cell) {
			inputs = new Vector<Cell>();
			inputs.add((Cell) value);
		} else {
			throw new ScriptException("values for row must be List<Cell> or Cell");
		}

		List<Cell> adds = new Vector<Cell>();
		for (Cell c : inputs) {
			if (ts != null)
				adds.add(CellUtil.createCell(row, CellUtil.cloneFamily(c), CellUtil.cloneQualifier(c), ts, (byte) 0,
						CellUtil.cloneValue(c)));
			else
				adds.add(CellUtil.createCell(row, CellUtil.cloneFamily(c), CellUtil.cloneQualifier(c), c.getTimestamp(),
						(byte) 0, CellUtil.cloneValue(c)));
		}

		container.addAll(adds);
	}

	@SuppressWarnings("unchecked")
	public static void addFamily(List<Cell> container, byte[] row, byte[] family, Object value, Long ts)
			throws ScriptException {
		List<Cell> inputs = null;
		if (value instanceof List)
			inputs = (List<Cell>) value;
		else if (value instanceof Cell) {
			inputs = new Vector<Cell>();
			inputs.add((Cell) value);
		} else {
			throw new ScriptException("values for row must be List<Cell> or Cell");
		}

		List<Cell> adds = new Vector<Cell>();
		for (Cell c : inputs) {
			if (ts != null)
				adds.add(CellUtil.createCell(row, family, CellUtil.cloneQualifier(c), ts, (byte) 0,
						CellUtil.cloneValue(c)));
			else
				adds.add(CellUtil.createCell(row, family, CellUtil.cloneQualifier(c), c.getTimestamp(), (byte) 0,
						CellUtil.cloneValue(c)));
		}

		container.addAll(adds);
	}

	public static void addColumn(List<Cell> container, byte[] row, byte[] family, byte[] column, Object value, Long ts)
			throws ScriptException {
		byte[] inputs = null;

		if (value instanceof byte[])
			inputs = (byte[]) value;
		else if (value instanceof Cell) {
			inputs = CellUtil.cloneValue((Cell) value);
		} else {
			throw new ScriptException("values for row must be Cell or byte[]");
		}

		if (ts != null)
			container.add(CellUtil.createCell(row, family, column, ts, (byte) 0, inputs));
		else if (value instanceof Cell)
			container.add(CellUtil.createCell(row, family, column, ((Cell) value).getTimestamp(), (byte) 0, inputs));
		else
			container.add(CellUtil.createCell(row, family, column, -1L, (byte) 0, inputs));
	}

	public static void assiRow(List<Cell> container, byte[] row, Object value, Long ts) throws ScriptException {
		removeRow(container, row);
		addRow(container, row, value, ts);
	}

	public static void assiFamily(List<Cell> container, byte[] row, byte[] family, Object value, Long ts)
			throws ScriptException {
		removeFamily(container, row, family);
		addFamily(container, row, family, value, ts);
	}

	public static void assiColumn(List<Cell> container, byte[] row, byte[] family, byte[] column, Object value, Long ts)
			throws ScriptException {
		removeColumn(container, row, family, column);
		addColumn(container, row, family, column, value, ts);
	}
}
