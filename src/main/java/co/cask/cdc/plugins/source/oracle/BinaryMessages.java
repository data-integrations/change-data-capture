package co.cask.cdc.plugins.source.oracle;

import co.cask.cdap.api.common.Bytes;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

/**
 * Utility methods for dealing with binary messages.
 */
public class BinaryMessages {
  private BinaryMessages() {
    // utility class
  }

  @Nonnull
  static byte[] getBytesFromBinaryMessage(Object message) {
    if (message instanceof ByteBuffer) {
      ByteBuffer bb = (ByteBuffer) message;
      return Bytes.toBytes(bb);
    } else {
      return (byte[]) message;
    }
  }
}
