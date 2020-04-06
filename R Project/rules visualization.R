# https://cran.r-project.org/web/packages/arulesViz/vignettes/arulesViz.pdf

plot(apriori(transactions_union, parameter = list(support = .02, confidence = .5)))