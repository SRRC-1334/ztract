       01  INVOICE-RECORD.
           05  INVOICE-ID        PIC 9(10).
           05  INVOICE-DATE      PIC 9(8).
           05  VENDOR-ID         PIC X(10).
           05  DEPT-COUNT        PIC 9(2).
           05  DEPARTMENTS       OCCURS 1 TO 5 TIMES
                                 DEPENDING ON DEPT-COUNT.
               10  DEPT-CODE     PIC X(4).
               10  DEPT-NAME     PIC X(20).
               10  ITEM-COUNT    PIC 9(2).
               10  ITEMS         OCCURS 1 TO 4 TIMES
                                 DEPENDING ON ITEM-COUNT.
                   15  ITEM-ID   PIC X(8).
                   15  ITEM-DESC PIC X(20).
                   15  ITEM-QTY  PIC 9(5).
                   15  ITEM-COST PIC S9(7)V99 COMP-3.
           05  INVOICE-TOTAL     PIC S9(11)V99 COMP-3.
           05  CURRENCY          PIC X(3).
           05  STATUS            PIC X(1).
