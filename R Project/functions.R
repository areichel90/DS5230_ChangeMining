get_num_transactions_per_partition <- function(df){
  df %>% select(partition, transaction) %>% 
    group_by(partition, transaction) %>% 
    summarize() %>% group_by(partition) %>% tally() %>% rename(num_transactions = n)
}

# Break dataframe up into months and generate list of transaction tables for each month
get_transactions_from_df <- function(x){
  as(split(as.data.frame(x)[,item_col],as.data.frame(x)[,transaction_col]), "transactions")
}

get_partitioned_transactions_from_df <- function(df){
  lapply(split(df,f=df[partition_col]),get_transactions_from_df)
}

get_partitioned_rules <- function(partitioned_transactions, minsup, minconf) {
  lapply(partitioned_transactions,function(transaction_set){
    apriori(transaction_set, parameter=list(support=minsup,confidence=minconf), control=list(verbose=F))
  })
}

get_all_rules <- function(partitioned_rules){
  union_rules <- partitioned_rules[[1]]
  for(x in partitioned_rules[2:length(partitioned_rules)]){
    union_rules <- arules::union(union_rules,x)
  }
  union_rules
}

filter_and_get_all_rules_measures <- function(all_rules, all_transactions, minsup, minconf){
  df <- as.data.frame(interestMeasure(all_rules, measure=c("support","confidence", "lift"),transactions=all_transactions,reuse=F)) %>% 
    mutate(rule = labels(all_rules))
  df %>% filter(support > minsup,confidence > minconf)
}

get_partitioned_rules_measures_df <- function(all_rules, partitioned_transactions,all_rules_measures_df,transactions_per_partition_df){
  df <- lapply(partitioned_transactions,function(x){
    interestMeasure(all_rules, measure=c("support","confidence","count","lift","coverage"),transactions=x,reuse=F) %>% 
      mutate(antecedent_count = coverage * length(x)) %>% mutate(rule = labels(all_rules))
  })
  df <- as.data.frame(rbindlist(df,fill=T,idcol=T)) %>% rename(rule_count = count)
  
  #df$rule <- rep(labels(all_rules),length(partitioned_transactions))
  
  # Filter down to rules that exist as valid rules in the global set
  df %>% inner_join(all_rules_measures_df %>% select(rule)) %>%
    # Include count of (B|~A) for chi squared test
    mutate(inverse_count = antecedent_count - rule_count) %>% filter(support > 0,confidence > 0) %>% 
    # Include count of invoices per month
    rename(partition = `.id`) %>% left_join(transactions_per_partition_df)
}

test_statistic <- function(p_hat,min_val,n){
  (p_hat - min_val) / (sqrt((min_val*(1-min_val))/n))
}

get_semi_stable_df <- function(all_rules_measures_df,partitioned_rules_measures_df, minsup, minconf,critical_z,num_partitions){
  partitioned_rules_measures_df %>% 
    mutate(conf_test_statistic = test_statistic(confidence,minconf,antecedent_count)) %>%
    mutate(supp_test_statistic = test_statistic(support,minsup,antecedent_count)) %>%
    # Remove any entries that are significantly below min_conf
    filter(conf_test_statistic > critical_z, supp_test_statistic > critical_z) %>%
    # Count how many time periods the rule is still associated with
    group_by(rule) %>% tally() %>% filter(n >= num_partitions) %>% 
    inner_join(all_rules_measures_df) %>% select(-n)
}

get_stable_df <- function(partitioned_rules_measures_df, all_rules_measures_df, semi_stable_df, confidence_val){
  if(nrow(semi_stable_df) > 0)
  {
  # Evaluate stable rules based on confidence: count of (AB) vs (B without A):
  conf_labels_split <- split(partitioned_rules_measures_df %>% select(rule,rule_count, inverse_count),f=partitioned_rules_measures_df$rule)
  supp_labels_split <- split(partitioned_rules_measures_df %>% select(rule,rule_count, num_transactions),f=partitioned_rules_measures_df$rule)
  
  p_values_conf_df <- rownames_to_column(as.data.frame(sapply(conf_labels_split,function(x){chisq.test(x %>% select(-rule))$p.value}))) %>% rename(rule = rowname,p_value_conf = 2)
  p_values_supp_df <- rownames_to_column(as.data.frame(sapply(supp_labels_split,function(x){chisq.test(x %>% select(-rule))$p.value}))) %>% rename(rule = rowname,p_value_supp = 2)
  
  # Stable rules must also be semi-stable rules. Drop rules that reject the null hypothesis that the observations are homogenius
  semi_stable_df %>% inner_join(p_values_conf_df) %>% inner_join(p_values_supp_df) %>% filter(p_value_conf > (1-confidence_val), p_value_supp > (1-confidence_val)) %>% 
     select(-p_value_conf, -p_value_supp)
  }else{
    data.frame()
  }
}

get_ci_graph_df <- function(partitioned_rules_measures_df, stable_df, confidence_val, partition_levels){
  df <- partitioned_rules_measures_df %>% 
    inner_join(stable_df %>% select(rule)) %>% 
    rowwise() %>%
    mutate(ci = exactci(rule_count, num_transactions, confidence_val)) %>% 
    mutate(partition = factor(partition, levels=partition_levels))
  
  df$ci_low <- as.vector(sapply(df$ci, function(x)x[1]))
  df$ci_high <- as.vector(sapply(df$ci, function(x)x[2]))
  df
}

get_change_mining_results <- function(df, minsup, minconf, confidence_val){
  
  all_transactions <- get_transactions_from_df(df)
  partitioned_transactions <- get_partitioned_transactions_from_df(df)
  all_rules <- get_all_rules(get_partitioned_rules(partitioned_transactions, minsup, minconf))
  all_rules_measures_df <- filter_and_get_all_rules_measures(all_rules, all_transactions, minsup, minconf)
  partitioned_rules_measures_df <- get_partitioned_rules_measures_df(all_rules, partitioned_transactions,all_rules_measures_df,get_num_transactions_per_partition(df))
  semi_stable_df <- get_semi_stable_df(all_rules_measures_df,partitioned_rules_measures_df, minsup, minconf,critical_z,length(unique(df$partition)))
  stable_df <- get_stable_df(partitioned_rules_measures_df, all_rules_measures_df, semi_stable_df, confidence_val)

  list(semi_stable_df=semi_stable_df, 
       stable_df=stable_df, 
       all_rules_measures_df=all_rules_measures_df,
       partitioned_rules_measures_df=partitioned_rules_measures_df,
       partitioned_transactions=partitioned_transactions,
       all_transactions=all_transactions)
}

get_rule_type_counts <- function(minsups, df, minconf, confidence_val){
  
  results <- mapply(FUN=get_change_mining_results, minsup=minsups,MoreArgs=list(df=df, minconf=minconf, confidence_val=confidence_val))
  
  frequent_rules <- sapply(seq(length(minsups)),function(x) nrow(results[,x]$all_rules_measures_df))
  semi_stable_rules <-  sapply(seq(length(minsups)),function(x) nrow(results[,x]$semi_stable_df))
  stable_rules <-  sapply(seq(length(minsups)),function(x) nrow(results[,x]$stable_df))
  
  data.frame(Frequent=frequent_rules,`Semi Stable`=semi_stable_rules,Stable=stable_rules,minsup=minsups) %>% pivot_longer(-minsup,names_to = "Rule Type") %>% rename(`Rule Count` = value)
}

get_partition_dist <- function(par1,par2,minsup){
  
  par1_items <- eclat(par1, parameter = list(supp = minsup), control = list(verbose = F))
  par2_items <- eclat(par2, parameter = list(supp = minsup), control = list(verbose = F))
  
  1-(length(arules::intersect(items(par1_items),items(par2_items)))/length(arules::union(items(par1_items),items(par2_items))))
}
