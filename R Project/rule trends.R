library(arules)
library(tidyverse)
library(data.table)
library(readxl)

df <- read_excel("..\\data\\online_retail_II.xlsx") %>%
  rename(InvoiceNo = Invoice, UnitPrice = Price, CustomerID = `Customer ID`) %>%
  mutate(isRefund = grepl("C", InvoiceNo) | Quantity < 0) %>%
  # Convert datetime to date
  mutate(InvoiceDate = as.Date(InvoiceDate, format = "%m/%d/%Y %H:%M")) %>%
  # Extract month
  mutate(InvoiceYearMonth = paste0(year(InvoiceDate),month(InvoiceDate)))


# Break dataframe up into months and generate list of transaction tables for each month

get_transactions <- function(x){
  as(split(as.data.frame(x)[,"Description"],as.data.frame(x)[,"InvoiceNo"]), "transactions")
}

transactions <- lapply(split(df,f=df$InvoiceYearMonth),get_transactions)

# Explore support - .04 seems like a good cutoff
supports <- c(.02,.03,.04,.05,.06,.07,.08,.09,.1)
itemset_df <- data.frame(support = rep(supports,length(transactions)))
itemset_df$time_partition <- as.vector(sapply((names(transactions)),function(x) rep(x,length(support))))
itemset_df$frequent_items <- as.vector(sapply(transactions,function(transaction_set){
  sapply(supports,function(x){
    print(transaction_set)
    length(eclat(transaction_set, parameter = list(supp = x, minlen=1))) 
    })
}))

ggplot(itemset_df) + geom_col(aes(x=factor(support),y=frequent_items,fill=time_partition), position="dodge")

# Explore rules
rules <- lapply(transactions,function(transaction_set){
  apriori(transaction_set, parameter=list(support=.04,confidence=.5))
          })

# Merge rules together into a master set of rules
union_rules <- rules[[1]]
for(x in rules[2:length(rules)]){
  union_rules <- arules::union(union_rules,x)
}

# evaluate support & confidence for these rules against all time periods
measures <- lapply(transactions,function(x){interestMeasure(union_rules, measure=c("support","confidence"),transactions=x,reuse=F)})

measures_df <- as.data.frame(rbindlist(measures,fill=T,idcol=T))
measures_df$label <- rep(labels(union_rules),length(transactions))

