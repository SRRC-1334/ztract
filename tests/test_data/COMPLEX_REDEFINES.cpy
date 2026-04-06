       01  MULTI-SEGMENT-RECORD.
           05  SEGMENT-ID              PIC X(2).
           05  COMMON-KEY              PIC 9(10).
           05  COMMON-DATE             PIC 9(8).
           05  SEGMENT-DATA.
               10  CUSTOMER-SEGMENT.
                   15  CUST-NAME       PIC X(40).
                   15  CUST-ADDR       PIC X(60).
                   15  CUST-CITY       PIC X(30).
                   15  CUST-ZIP        PIC X(10).
                   15  CUST-PHONE      PIC X(15).
                   15  FILLER          PIC X(45).
               10  ACCOUNT-SEGMENT REDEFINES CUSTOMER-SEGMENT.
                   15  ACCT-TYPE       PIC X(3).
                   15  ACCT-STATUS     PIC X(1).
                   15  ACCT-BALANCE    PIC S9(11)V99 COMP-3.
                   15  ACCT-LIMIT      PIC S9(11)V99 COMP-3.
                   15  ACCT-OPEN-DATE  PIC 9(8).
                   15  ACCT-BRANCH     PIC X(10).
                   15  FILLER          PIC X(155).
               10  PAYMENT-SEGMENT REDEFINES CUSTOMER-SEGMENT.
                   15  PAY-AMOUNT      PIC S9(11)V99 COMP-3.
                   15  PAY-CURRENCY    PIC X(3).
                   15  PAY-REF         PIC X(20).
                   15  PAY-STATUS      PIC X(1).
                   15  PAY-VALUE-DATE  PIC 9(8).
                   15  FILLER          PIC X(158).
