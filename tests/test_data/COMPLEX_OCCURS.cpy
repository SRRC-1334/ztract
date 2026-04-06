       01  ORDER-RECORD.
           05  ORDER-ID                PIC 9(10).
           05  ORDER-DATE              PIC 9(8).
           05  CUSTOMER-NR             PIC 9(10).
           05  ORDER-STATUS            PIC X(2).
           05  LINE-COUNT              PIC 9(3).
           05  ORDER-LINES OCCURS 0 TO 10 TIMES
               DEPENDING ON LINE-COUNT.
               10  LINE-ITEM-NR        PIC 9(5).
               10  LINE-PRODUCT        PIC X(20).
               10  LINE-QTY            PIC 9(5).
               10  LINE-PRICE          PIC S9(7)V99 COMP-3.
               10  LINE-AMOUNT         PIC S9(9)V99 COMP-3.
           05  ORDER-TOTAL             PIC S9(11)V99 COMP-3.
           05  FILLER                  PIC X(50).
