library(arules)
library(tidyverse)
library(data.table)
library(readxl)

minsup <- 

df <- read_excel("..\\data\\online_retail_II.xlsx") %>%
  rename(InvoiceNo = Invoice, UnitPrice = Price, CustomerID = `Customer ID`) %>%
  mutate(isRefund = grepl("C", InvoiceNo) | Quantity < 0) %>%
  # Convert datetime to date
  mutate(InvoiceDate = as.Date(InvoiceDate, format = "%m/%d/%Y %H:%M")) %>%
  # Extract month
  mutate(InvoiceYearMonth = paste0(year(InvoiceDate),month(InvoiceDate))) %>%
  # Consider 2010 data only
  filter(grepl("2010",InvoiceYearMonth))


# Break dataframe up into months and generate list of transaction tables for each month
get_transactions <- function(x){
  as(split(as.data.frame(x)[,"Description"],as.data.frame(x)[,"InvoiceNo"]), "transactions")
}

transactions <- lapply(split(df,f=df$InvoiceYearMonth),get_transactions)
time_periods <- length(transactions)
transactions_union <- get_transactions(df)

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
minsup <- .02
minconf <- .3
critical_z <- -1.65 # equates to 95% confidence

rules <- lapply(transactions,function(transaction_set){
  apriori(transaction_set, parameter=list(support=minsup,confidence=minconf))
          })
# Merge rules together into a master set of rules
union_rules <- rules[[1]]
for(x in rules[2:length(rules)]){
  union_rules <- arules::union(union_rules,x)
}

# evaluate support & confidence for these rules against the entire time period
measures_union_df <- as.data.frame(interestMeasure(union_rules, measure=c("support","confidence"),transactions=transactions_union,reuse=F))
measures_union_df$label <- labels(union_rules)
measures_union_df <- measures_union_df %>% filter(support > minsup,confidence > minconf)

# evaluate support & confidence for these rules against all individual time periods
measures <- lapply(transactions,function(x){
  interestMeasure(union_rules, measure=c("support","confidence","count","coverage"),transactions=x,reuse=F) %>% 
    mutate(antecedent_count = coverage * length(x))
  })
measures_df <- as.data.frame(rbindlist(measures,fill=T,idcol=T)) 
measures_df$label <- rep(labels(union_rules),length(transactions))
# Filter down to rules that exist as valid rules in the global set
measures_df <- measures_df %>% inner_join(measures_union_df %>% select(label))

test_statistic <- function(p_hat,min_val,n){
  (p_hat - min_val) / (sqrt((min_val*(1-min_val))/n))
}

semi_stable_df <- measures_df %>% rename(rule_count = count) %>%
  #filter(support > global_minsup,confidence>global_minconf) %>%
  mutate(conf_test_statistic = test_statistic(confidence,minconf,antecedent_count)) %>%
  mutate(supp_test_statistic = test_statistic(support,minsup,antecedent_count)) %>%
  # Remove any entries that are significantly below min_conf
  filter(conf_test_statistic > critical_z, supp_test_statistic > critical_z) %>%
  # Count how many time periods the rule is still associated with
  group_by(label) %>% tally() %>% filter(n >= time_periods)

# semi stable rules alongside global support & confidence
View(semi_stable_df %>% inner_join(measures_union_df))

