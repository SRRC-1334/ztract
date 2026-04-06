       01  NUMERIC-TEST-RECORD.
           05  REC-ID                  PIC 9(8).
           05  DISPLAY-UNSIGNED        PIC 9(9).
           05  DISPLAY-SIGNED          PIC S9(9).
           05  DISPLAY-DECIMAL         PIC 9(7)V99.
           05  DISPLAY-SIGNED-DEC      PIC S9(7)V99.
           05  COMP3-UNSIGNED          PIC 9(9) COMP-3.
           05  COMP3-SIGNED            PIC S9(9) COMP-3.
           05  COMP3-DECIMAL           PIC 9(7)V99 COMP-3.
           05  COMP3-SIGNED-DEC        PIC S9(7)V99 COMP-3.
           05  COMP3-LARGE             PIC S9(15)V99 COMP-3.
           05  COMP-SHORT              PIC S9(4) COMP.
           05  COMP-LONG               PIC S9(9) COMP.
           05  COMP-VERY-LONG          PIC S9(18) COMP.
           05  ALPHA-FIELD             PIC X(20).
           05  ALPHA-MAX               PIC X(100).
           05  ALPHA-ZERO              PIC X(1).
           05  FILLER                  PIC X(50).
