package sssj.io;

import sssj.Vector;

import com.google.common.base.Function;

public class ParserFactory {

  public static enum Format {
    SSSJ {
      @Override
      public Function<String, Vector> getRecordParser() {
        return new Function<String, Vector>() {
          public Vector apply(String input) {
            String[] tokens = input.split(" ");
            Long ts = Long.parseLong(tokens[0]);
            Vector result = new Vector(ts);
            for (int i = 1; i < tokens.length; i += 2) {
              int key = Integer.parseInt(tokens[i]);
              double val = Double.parseDouble(tokens[i + 1]);
              result.put(key, val);
            }
            return Vector.l2normalize(result);
          }
        };
      }
    },

    VW {
      @Override
      public Function<String, Vector> getRecordParser() {
        // TODO Auto-generated method stub
        return null;
      }
    };

    abstract public Function<String, Vector> getRecordParser();
  }

}
