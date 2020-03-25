library(tidyverse)
#df1 <- read_csv("data\\online_retail_I.csv")
# Not sure why II has the same data as I
df <- read_csv("data\\online_retail_II.csv") %>%
  rename(InvoiceNo = Invoice, UnitPrice = Price, CustomerID = `Customer ID`) %>%
  mutate(isRefund = grepl("C", InvoiceNo) | Quantity < 0)

# 25900 unique invoices
df %>% count(InvoiceNo) %>% count()

# 3836 refunds

# Generate refunds column
df <- df %>% 
#df %>% filter(grepl("C", InvoiceNo)) %>% count(InvoiceNo) %>% count()

# 20.9 items per basket
df %>% count(Invoice) %>% summarise(avg_items=mean(n))

# 4070 unique items
df %>% count(StockCode) %>% count()

# A few items with anomylous entries related to bad debt
View(df %>% filter(grepl("bad debt", tolower(Description))))
# All of the negative quantities have a unit price of 0 (or are a refund)
View(df %>% filter(!grepl("C", InvoiceNo)) %>% filter(Quantity < 0))
