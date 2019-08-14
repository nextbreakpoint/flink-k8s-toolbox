package com.nextbreakpoint.flinkoperator.common.crd;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.lang.reflect.Type;

public class DateTimeSerializer implements JsonDeserializer<DateTime>, JsonSerializer<DateTime> {
   private static final DateTimeFormatter TIME_FORMAT = ISODateTimeFormat.dateTimeNoMillis();

   @Override
   public DateTime deserialize(final JsonElement je, final Type type, final JsonDeserializationContext jdc) throws JsonParseException {
      final String dateAsString = je.getAsString();
      if (dateAsString.length() == 0) {
         return null;
      } else {
         return TIME_FORMAT.parseDateTime(dateAsString);
      }
   }

   @Override
   public JsonElement serialize(final DateTime src, final Type typeOfSrc, final JsonSerializationContext context) {
      String retVal;
      if (src == null) {
         retVal = "";
      } else {
         retVal = TIME_FORMAT.print(src);
      }
      return new JsonPrimitive(retVal);
   }
}
