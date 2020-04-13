get_num_transactions_per_partition <- function(df){
  df %>% select(partition, transaction) %>% 
    group_by(partition, transaction) %>% 
    summarize() %>% group_by(partition) %>% tally() %>% rename(num_transactions = n)
}

# Break dataframe up into months and generate list of transaction tables for each month
get_transactions_from_df <- function(x){
  t <- as(split(as.data.frame(x)[,item_col],as.data.frame(x)[,transaction_col]), "transactions")
  df_weights <- x %>% group_by(transaction) %>% 
    summarise(weight_abs = sum(weight_abs)) %>% 
    # Attempt to cap outliers at 1500. TODO make this more robust
    mutate(weight_abs = if_else(weight_abs > 1500, 1500, weight_abs)) %>%
    mutate(weight = weight_abs/sum(weight_abs))
  #t@itemsetInfo$weights <- df_weights$weight
  transactionInfo(t) <- data.frame(weight_abs = df_weights$weight_abs, weight = df_weights$weight, transactionId = df_weights$transaction )
  t
}

get_partitioned_transactions_from_df <- function(df){
  lapply(split(df,f=df[partition_col]),get_transactions_from_df)
}

get_partitioned_rules <- function(partitioned_transactions, minsup, minconf, useWeights = F) {
  if (useWeights == F){
    lapply(partitioned_transactions,function(transaction_set){
      apriori(transaction_set, parameter=list(support=minsup,confidence=minconf, minlen = 2), control=list(verbose=F))
    })  
  } else
  {
    lapply(partitioned_transactions,function(transaction_set){
      # Uses weighted eclat rather than apriori
      ruleInduction(weclat(transaction_set,parameter = list(support = minsup, minlen = 2), control = list(verbose = F)), transaction_set, confidence = minconf)
    })
  }
}

get_all_rules <- function(partitioned_rules){
  union_rules <- partitioned_rules[[1]]
  for(x in partitioned_rules[2:length(partitioned_rules)]){
    union_rules <- arules::union(union_rules,x)
  }
  union_rules
}

filter_and_get_all_rules_measures <- function(all_rules, all_transactions, minsup, minconf){
  quality(all_rules) %>% mutate(rule = labels(all_rules)) %>% filter(support > minsup,confidence > minconf)
}

get_partitioned_rules_measures_df <- function(all_rules, partitioned_transactions,all_rules_measures_df, useWeights){
  
  df <- lapply(partitioned_transactions,function(x){
      # TODO there's some sort of error present when using weights. Rule 4541 has 0 support in June if we evaluate all_rules[4541] but .049 support in June if we run support()[4541]
    data.frame(list(support = support(all_rules, x, weighted = useWeights))) %>% 
      # Calculating count like this to consider weighted support
      mutate(support_count = support * as.double(nrow(x))) %>% 
      mutate(lhs_support = support(lhs(all_rules), x, weighted = useWeights )) %>%
      mutate(rhs_support = support(rhs(all_rules), x, weighted = useWeights )) %>%
      mutate(lhs_support_count = lhs_support * as.double(nrow(x))) %>%
      mutate(rule = labels(all_rules)) %>%
      mutate(rhs_support_count = rhs_support * as.double(nrow(x))) %>%
      mutate(num_transactions = nrow(x))
  })

  df <- as.data.frame(rbindlist(df,fill=T,idcol=T)) %>% 
    rename(partition = `.id`) %>% 
    mutate(confidence = support / lhs_support) %>%
    mutate(lift = (support / (lhs_support*rhs_support))) %>%
    # Filter down to rules that exist as valid rules in the global set
    inner_join(all_rules_measures_df %>% select(rule)) %>%
    # Include count of (B|~A) for chi squared test
    mutate(inverse_count = lhs_support_count - support_count) %>%
    # Include count of invoices per month
    filter(support > 0, rhs_support > 0, lhs_support > 0)
  
  df
}

test_statistic <- function(p_hat,min_val,n){
  (p_hat - min_val) / (sqrt((min_val*(1-min_val))/n))
}

get_semi_stable_df <- function(all_rules_measures_df,partitioned_rules_measures_df, minsup, minconf,critical_z){
  browser()
  num_partitions <- length(unique(partitioned_rules_measures_df$partition))
  partitioned_rules_measures_df %>% 
    mutate(conf_test_statistic = test_statistic(confidence,minconf,lhs_support_count)) %>%
    mutate(supp_test_statistic = test_statistic(support,minsup,support_count)) %>%
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
  conf_labels_split <- split(partitioned_rules_measures_df %>% select(rule,support_count, inverse_count),f=partitioned_rules_measures_df$rule)
  supp_labels_split <- split(partitioned_rules_measures_df %>% select(rule,support_count, num_transactions),f=partitioned_rules_measures_df$rule)
  
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
    mutate(ci = exactci(support_count, num_transactions, confidence_val)) %>% 
    mutate(partition = factor(partition, levels=partition_levels))
  
  df$ci_low <- as.vector(sapply(df$ci, function(x)x[1]))
  df$ci_high <- as.vector(sapply(df$ci, function(x)x[2]))
  df
}

get_change_mining_results <- function(df, minsup, minconf, confidence_val = .95, useWeights = F){
  
  all_transactions <- get_transactions_from_df(df)
  partitioned_transactions <- get_partitioned_transactions_from_df(df)
  partitioned_rules <- get_partitioned_rules(partitioned_transactions, minsup, minconf, useWeights)
  all_rules <- get_all_rules(partitioned_rules)
  all_rules_measures_df <- filter_and_get_all_rules_measures(all_rules, all_transactions, minsup, minconf)
  partitioned_rules_measures_df <- get_partitioned_rules_measures_df(all_rules, partitioned_transactions,all_rules_measures_df, useWeights)
  semi_stable_df <- get_semi_stable_df(all_rules_measures_df,partitioned_rules_measures_df, minsup, minconf,critical_z)
  
  stable_df <- get_stable_df(partitioned_rules_measures_df, all_rules_measures_df, semi_stable_df, confidence_val)

  list(semi_stable_df=semi_stable_df, 
       stable_df=stable_df, 
       all_rules = all_rules,
       partitioned_rules = partitioned_rules,
       all_rules_measures_df=all_rules_measures_df,
       partitioned_rules_measures_df=partitioned_rules_measures_df,
       partitioned_transactions=partitioned_transactions,
       all_transactions=all_transactions,
       semi_stable_rules=all_rules[which(labels(all_rules) %in% semi_stable_df$rule)])
}

get_rule_type_counts <- function(minsups, df, minconf, confidence_val, useWeights = F){
  
  results <- mapply(FUN=get_change_mining_results, minsup=minsups,MoreArgs=list(df=df, minconf=minconf, confidence_val=confidence_val, useWeights = useWeights))
  
  frequent_rules <- sapply(seq(length(minsups)),function(x) nrow(results[,x]$all_rules_measures_df))
  semi_stable_rules <-  sapply(seq(length(minsups)),function(x) nrow(results[,x]$semi_stable_df))
  stable_rules <-  sapply(seq(length(minsups)),function(x) nrow(results[,x]$stable_df))
  
  data.frame(Frequent=frequent_rules,`Semi Stable`=semi_stable_rules,Stable=stable_rules,minsup=minsups) %>% pivot_longer(-minsup,names_to = "Rule Type") %>% rename(`Rule Count` = value)
}

get_partition_dist <- function(par1,par2,minsup, useWeights = F, target = "frequent itemsets"){
  if(useWeights == F)
  {
  par1_items <- eclat(par1, parameter = list(support = minsup, target = target), control = list(verbose = F))
  par2_items <- eclat(par2, parameter = list(support = minsup, target = target), control = list(verbose = F))
  } else
  {
    par1_items <- weclat(par1, parameter = list(support = minsup, target = target), control = list(verbose = F))
    par2_items <- weclat(par2, parameter = list(support = minsup, target = target), control = list(verbose = F))
  }
  #browser()
  (1-(length(arules::intersect((par1_items),(par2_items)))/length(arules::union((par1_items),(par2_items)))))
}

get_extension_sets <- function(month1, month2)
{
  t0_freq_itemsets <- eclat(month1, parameter = list(supp = minsup, minlen=1), control=list(verbose=F)) 
  t1_freq_itemsets <- eclat(month2, parameter = list(supp = minsup, minlen=2), control=list(verbose=F)) 
  
  t1_itemsets_not_in_t0 <- t1_freq_itemsets[support(t1_freq_itemsets, month1) == 0]
  
  df_extension <- (as.data.frame(is.subset(t0_freq_itemsets, t1_itemsets_not_in_t0, sparse=F)) %>% rownames_to_column() %>% rename(t0_items = rowname) %>% pivot_longer(-t0_items,names_to="t1_items", values_to="t0_in_t1")) %>% filter(t0_in_t1 == T) %>% select(-t0_in_t1)
  df_extension
  
}

get_num_extension_sets <- function(month1, month2)
{
  nrow(get_extension_sets(month1, month2))
}
