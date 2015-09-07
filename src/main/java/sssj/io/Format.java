package sssj.io;

import sssj.base.Vector;

import com.google.common.base.Function;

public enum Format {
  SSSJ {
    @Override
    public Function<String, Vector> getRecordParser() {
      return new Function<String, Vector>() {
        public Vector apply(String input) {
          String[] tokens = input.split("\\s");
          Long ts = Long.parseLong(tokens[0]);
          Vector result = new Vector(ts);
          for (int i = 1; i < tokens.length; i += 2) {
            int key = Integer.parseInt(tokens[i]);
            double val = Double.parseDouble(tokens[i + 1]);
            result.put(key, val);
          }
          return result;
        }
      };
    }
  },

  SVMLIB {
    @Override
    public Function<String, Vector> getRecordParser() {
      return new Function<String, Vector>() {
        public Vector apply(String input) {
          String[] tokens = input.split("\\s");
          // ignore class label tokens[0]
          Vector result = new Vector();
          for (int i = 1; i < tokens.length; i++) {
            String[] parts = tokens[i].split(":");
            int key = Integer.parseInt(parts[0]);
            double val = Double.parseDouble(parts[1]);
            result.put(key, val);
          }
          return result;
        }
      };
    }
  },

  VW {
    @Override
    public Function<String, Vector> getRecordParser() {
      return new Function<String, Vector>() {
        public Vector apply(String input) {
          String[] tokens = input.split("\\s");
          // ignore class label tokens[0] and namespace tokens[1]
          Vector result = new Vector();
          for (int i = 2; i < tokens.length; i++) {
            String[] parts = tokens[i].split(":");
            int key = Integer.parseInt(parts[0]);
            double val = Double.parseDouble(parts[1]);
            result.put(key, val);
          }
          return result;
        }
      };
    }
  },

  BINARY {
    @Override
    public Function<String, Vector> getRecordParser() {
      throw new UnsupportedOperationException("Binary format does not need a parser");
    }
  };

  abstract public Function<String, Vector> getRecordParser();
}