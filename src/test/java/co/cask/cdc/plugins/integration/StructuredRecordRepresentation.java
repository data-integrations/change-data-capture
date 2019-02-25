package co.cask.cdc.plugins.integration;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.format.StructuredRecordStringConverter;
import org.assertj.core.presentation.StandardRepresentation;

import java.io.IOException;

public class StructuredRecordRepresentation extends StandardRepresentation {
  @Override
  public String toStringOf(Object object) {
    try {
      if (object instanceof StructuredRecord) {
        return StructuredRecordStringConverter.toJsonString((StructuredRecord) object);
      }
      return super.toStringOf(object);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
