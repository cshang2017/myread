package org.apache.flink.table.data.vector;

/**
 * Bytes column vector to get {@link Bytes}, it include original data and offset and length.
 * The data in {@link Bytes} maybe reuse.
 */
public interface BytesColumnVector extends ColumnVector {
	Bytes getBytes(int i);

	/**
	 * Bytes data.
	 */
	class Bytes {
		public final byte[] data;
		public final int offset;
		public final int len;

		public Bytes(byte[] data, int offset, int len) {
			this.data = data;
			this.offset = offset;
			this.len = len;
		}

		public byte[] getBytes() {
			if (offset == 0 && len == data.length) {
				return data;
			}
			byte[] res = new byte[len];
			System.arraycopy(data, offset, res, 0, len);
			return res;
		}
	}
}
