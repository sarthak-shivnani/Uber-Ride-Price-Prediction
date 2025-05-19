# ---------------------------Google API Call for Map distance----------------------------

# Read raw uber data file
set.seed(1)
raw_data <- read.csv("uber.csv")
dim(raw_data)

# User Input
# API_key <- 'Enter_your_api_key'

# function to get map distance from Google Maps API
get_map_distance <- function(pickup, dropoff, API_key)
{
  # Construct the URL
  url <- paste0("https://maps.googleapis.com/maps/api/distancematrix/json?origins=",
                pickup, "&destinations=", dropoff, "&key=", API_key)
  
  # Make the request
  response <- httr::GET(url)
  
  # Parse the response
  response_content <- httr::content(response, as = "text")
  response_json <- jsonlite::fromJSON(response_content)
  
  # Check the status and extract the distance
  if (response_json$status == "OK")
  {
    # Get the distance in meters
    distance_meters <- response_json$rows$elements[[1]]$distance$value
    # Convert the distance to kilometers
    distance_miles <- distance_meters / 1609.344
    print(paste("Distance:", distance_miles, "miles"))
  }
  else
  {
    print("Error:", response_json$error_message)
    distance_miles <- NULL
  }
  return(distance_miles)
}

# new_data <- head(raw_data)
new_data <- raw_data %>%
  mutate(pickup=paste(pickup_latitude, pickup_longitude, sep=','),
         dropoff=paste(dropoff_latitude, dropoff_longitude, sep=',')
  )

# final data
df <- new_data[new_data$pickup != new_data$dropoff, ]

# Set seed for reproducibility
set.seed(123)

# Function to split the data frame into batches of 20000 rows
split_into_batches <- function(df, batch_size) {
  split(df, ceiling(seq_along(rownames(df)) / batch_size))
}

# Split the data frame into batches of 20000 rows
batch_size <- 20000
batches <- split_into_batches(df, batch_size)

# Print the number of batches and the first few rows of each batch
length(batches)

# Define the function to process each batch and get the map distance using API call
process_batch <- function(batch) {
  batch$distance_miles <- apply(batch, 1, function(row){
    get_map_distance(
      pickup = row["pickup"]
      ,dropoff = row["dropoff"]
      ,API_key = API_key)
  })
  return(batch)
}

# Initialize an empty list to store the processed batches
processed_batches <- list()

# Process each batch and store the results in processed_batches
for (i in seq_along(batches)) {
  processed_batches[[i]] <- process_batch(batches[[i]], API_key)
  print(paste("Processed batch", i, "of", length(batches)))
}

# Combine all processed batches to form the final data
final_df <- do.call(rbind, processed_batches)
# write the csv to use later
write.csv(final_df, "uber_google_api_distance.csv")

# ---------------------------------------- FINAL CODE --------------------------------------


#--------------------------------------------------------------------
# Pre-processing
#--------------------------------------------------------------------


data <- read.csv("uber_google_api_distance.csv")
data <- distinct(data)

# The first column is key and the 2nd column is just time, but the column names are incorrect
# we don't need only time, as we have pickup_datetime as a separate feature
# drop the time column which is named incrrectly and name the key column correctly

data <- data %>%
  select(-key) %>%
  drop_na()

colnames(data)[colnames(data) == "X"] = "key"

# ----------- Append UberXL Flag ------------------------------------------------

data <- data %>%
  mutate(uberxl_flag = factor(ifelse((passenger_count>4), TRUE, FALSE)))

#filter data for non zero lat-long
data_long_lat_correction <- data %>%
  filter(pickup_longitude != 0 &
           pickup_latitude != 0 &
           dropoff_longitude != 0 &
           dropoff_latitude != 0,
         fare_amount > 1,
         pickup_longitude >= -180 & pickup_longitude <= 180 &
           dropoff_longitude >= -180 & dropoff_longitude <= 180 &
           pickup_latitude >= -90 & pickup_latitude <= 90 &
           dropoff_latitude >= -90 & dropoff_latitude <= 90
  ) %>%
  mutate(
    distance_miles = as.numeric(distance_miles)
  )

# split the pickup_datetime column and create a dateframe of 2 columns : date and time

split_time <- strsplit(data_long_lat_correction$pickup_datetime, " ")

split_df <- do.call(rbind, split_time)

split_df <- as.data.frame(split_df, stringsAsFactors = FALSE)
colnames(split_df) <- c("date", "time", "UTC")

# drop the UTC column
split_df <- subset(split_df, select = -UTC)

data_date_time <- cbind(data_long_lat_correction, split_df)

data_uber <- data_date_time %>%
  mutate(
    p_date = ymd(date),
    p_time = chron(times=time),
    day = weekdays(p_date),
    year = year(p_date),
    month = month(p_date),
    weekday = factor(ifelse((day=="Sunday" | day=="Saturday"), "No", "Yes")),
    seasons = factor(case_when(month %in% c(12, 1, 2) ~ "Winter",
                               month %in% c(3, 4, 5) ~ "Spring",
                               month %in% c(6, 7, 8) ~ "Summer",
                               month %in% c(9,10,11) ~ "Fall")),
    pickuptimeOfDay = factor(case_when(between(p_time, chron(times=c('06:00:00')),
                                               chron(times=c('11:59:59'))) ~ "Morning",
                                       between(p_time, chron(times=c('12:00:00')),
                                               chron(times=c('16:59:59'))) ~ "Afternoon",
                                       between(p_time, chron(times=c('17:00:00')),
                                               chron(times=c('20:59:59'))) ~ "Evening",
                                       between(p_time, chron(times=c('21:00:00')),
                                               chron(times=c('23:59:59'))) ~ "Night",
                                       between(p_time, chron(times=c('00:00:00')),
                                               chron(times=c('05:59:59'))) ~ "Night")),
    passenger_count = cut(passenger_count, breaks=c(0, 2, 4, 6))
  ) %>%
  select(-date, -time, -month,
         -pickup_datetime,
         -pickup, -dropoff,
         -p_time, -zipcode,
         -key, -day, -seasons,
         -pickup_longitude, -pickup_latitude,
         -dropoff_longitude, -dropoff_latitude)  %>%
  drop_na()


# ----------- Append Holiday Flag ---------------------------------------

library(timeDate)

# Get holidays for each year and combine into a single vector
years <- min(data_uber$year):max(data_uber$year)
us_holidays_list <- lapply(years, function(year) {
  as.Date(holidayNYSE(year))
})

# Combine them to create a data frame of holiday list and set flag to TRUE
all_holidays <- do.call(c, us_holidays_list)
us_holidays_df <- data.frame(p_date = all_holidays)
us_holidays_df$holiday_flag <- TRUE

# Append holidays to original data
data_uber <- data_uber %>%
  left_join(us_holidays_df, by = "p_date") %>%
  mutate(holiday_flag = factor(ifelse(!is.na(holiday_flag), TRUE, FALSE)) )%>%
  select(-p_date) %>%
  mutate(year=factor(year))

head(data_uber)
summary(data_uber)
dim(data_uber)

#--------------------------------------------------------------------
# Linear Models
#--------------------------------------------------------------------

linear_model <- lm(fare_amount~., data = train_data)
summary(linear_model)

yhat_lm <- predict(linear_model, test_data)

y_test <- test_data$fare_amount

test_mse_lm <- sqrt(mean((yhat_lm - y_test)^2))
test_mse_lm

#--------------------------------------------------------------------
# Stepwise regression
#--------------------------------------------------------------------
stepwise <- stepAIC(linear_model, direction = "both")
summary(stepwise)

yhat_sw <- predict(stepwise, test_data)

test_mse_sw <- sqrt(mean((yhat_sw - y_test)^2))
test_mse_sw


#--------------------------------------------------------------------
# Boosting - XGBoost & GBM
#--------------------------------------------------------------------

library(caret)

# Set seed for reproducibility
set.seed(1)

# Train-Test split
# Create training index
trainIndex <- createDataPartition(data_uber$fare_amount, p = 0.8, list = FALSE)

# Create the training and testing sets
trainData <- data_uber[trainIndex, ]
testData <- data_uber[-trainIndex, ]

# values that define how the train function must act
fit_control <- trainControl(
  method = "cv",
  number = 10,
  selectionFunction="oneSE")

#------------------------------ Boosting: GBM --------------------------------------------------

library(gbm)

# Boosting, optimizing over default grid for number of trees and depth
gbmfit <- train(fare_amount ~ ., data = data_uber,
                method = "gbm",
                trControl = fit_control,
                verbose = FALSE)

# Using a custom grid for tuning
gbm_grid <-  expand.grid(interaction.depth = c(1, 2, 3, 5, 10), # depth of tree
                         n.trees = c(150, 500, 1000), # number of trees
                         shrinkage = c(0.01, 0.1, 0.2), # lambda or shrinkage
                         n.minobsinnode = 10 # number of min obs. required for each node
)
gbmfit_2 <- train(fare_amount ~ ., data = data_uber, # . means all features
                  method = "gbm", # what ML model to use
                  trControl = fit_control,
                  tuneGrid = gbm_grid,
                  verbose = FALSE
)
print(gbmfit_2)
# RMSE was used to select the optimal model using  the one SE rule.
# The final values used for the model were n.trees = 150, interaction.depth = 1, shrinkage = 0.1
# and n.minobsinnode = 10; RMSE: 4.436574

print(gbmfit_2$results)

# Determine the max RMSE that's within one SE of best
best_ix = which.min(gbmfit_2$results$RMSE) # Gives index of the best i.e. min
best = gbmfit_2$results[best_ix,]
onese_max_RMSE = best$RMSE + best$RMSESD/sqrt(10)

# These are the parameter values within one SD:
onese_ixs = gbmfit_2$results$RMSE<onese_max_RMSE

print(gbmfit_2$results[onese_ixs,]) # We have taken the least complex of all of these (1st row)

# Visualization
plot(gbmfit_2)
ggplot(gbmfit_2)

gbm_plot_df = gbmfit_2$results
gbm_plot_df$n.trees = factor(gbm_plot_df$n.trees)

kcv=10
ggplot(aes(x=interaction.depth, y=RMSE, color=n.trees),
       data=gbm_plot_df) +
  facet_grid(~shrinkage, labeller = label_both) +
  geom_point() +
  geom_line() +
  geom_segment(aes(x=interaction.depth,
                   xend=interaction.depth,
                   y=RMSE-RMSESD/sqrt(kcv),
                   yend=RMSE+RMSESD/sqrt(kcv))) +
  geom_hline(yintercept = onese_max_RMSE, linetype='dotted') +
  xlab("Max Tree Depth") +
  ylab("RMSE (CV)") +
  scale_color_discrete(name = "Num Boosting Iter") +
  theme(legend.position="bottom")

# On our validation set:
gbm_yhat = predict(gbmfit_2, newdata=testData)

# Validation RMSE
sqrt(mean((testData$fare_amount - gbm_yhat)^2 )) # 4.374432

# Comparing variable importance
gbm_imp = varImp(gbmfit_2)
combined_df = data.frame(variable=rownames(gbm_imp$importance),
                         gbm = gbm_imp$importance$Overall)

#------------------------------ Boosting: XGBoost --------------------------------------------------

xgb_fit_control <- trainControl(
  method = "cv",
  number = 10,
  selectionFunction="oneSE"
)

# Tuning grid
xgb_grid <- expand.grid(nrounds = c(150, 500, 1000),  # Number of boosting rounds
                        max_depth = c(4, 6, 8),  # Maximum depth of a tree
                        eta = c(0.01, 0.1, 0.3),  # Learning rate
                        gamma = 0,  # Minimum loss reduction
                        colsample_bytree = 1,  # Subsample ratio of columns
                        min_child_weight = 10,  # Minimum sum of instance weight
                        subsample = c(0.8,1)  # Subsample ratio of the training instances
)

# Boosting, optimizing over default grid for number of trees and depth
xgbfit <- train(fare_amount ~ ., data = data_uber,
                method = "xgbTree",
                trControl = xgb_fit_control,
                tuneGrid = xgb_grid,
                verbose = FALSE)

print(xgbfit)
# RMSE was used to select the optimal model using  the one SE rule.
# The final values used for the model were nrounds = 50, max_depth = 4, eta = 0.1, gamma = 0,
# colsample_bytree = 1, min_child_weight = 10 and subsample = 0.8.
# RMSE -- 4.276331

print(xgbfit$results)

# On our validation set:
xgb_yhat = predict(xgbfit, newdata=testData)

# Validation RMSE
sqrt(mean((testData$fare_amount - xgb_yhat)^2 )) # 3.920847-- with oneSE, 4.003607 with Best

# Comparing variable importance
xgb_imp <- varImp(xgbfit, scale = TRUE)
plot(xgb_imp)
imp_df = data.frame(variable=rownames(xgb_imp$importance),
                    xgb = xgb_imp$importance$Overall)

#--------------------------------------------------------------------
# BART 
#--------------------------------------------------------------------

# BART implementation
library(BART)
set.seed(1)

# Split the data into training and testing sets
x <- data_uber %>%
  select(-fare_amount)
y <- data_uber$fare_amount
trainIndex <- createDataPartition(y, p = 0.8, list = FALSE)

x_train <- x[trainIndex, ]
x_test <- x[-trainIndex, ]
y_train <- y[trainIndex]
y_test <- y[-trainIndex]

#Model Training and Prediction
bartfit <- wbart(x.train =  x_train, y.train = y_train, x.test = x_test)
yhat_train <- bartfit$yhat.train.mean
yhat_test <- bartfit$yhat.test.mean

#Train and Validation RMSE
bart_train_rmse <- sqrt(mean((yhat_train - y_train)^2))
bart_test_rmse <- sqrt(mean((yhat_test - y_test)^2))
