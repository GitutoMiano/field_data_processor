class FieldDataProcessor:

    def __init__(self, config_params, logging_level="INFO"):
        self.db_path = config_params['db_path']
        self.sql_query = config_params['sql_query']
        self.columns_to_rename = config_params['columns_to_rename']
        self.values_to_rename = config_params['values_to_rename']
        self.weather_map_data = config_params['weather_mapping_csv']

        self.initialize_logging(logging_level)

        # We create empty objects to store the DataFrame and engine in
        self.df = None
        self.engine = None

    def initialize_logging(self, logging_level):
        """
        Sets up logging for this instance of FieldDataProcessor.
        """
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False

        # Set logging level
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO

        self.logger.setLevel(log_level)

        if not self.logger.handlers:
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)
    
    def ingest_sql_data(self):
        self.logger.info("Loading data...")

        # Create the database engine
        self.engine = create_db_engine(self.db_path)

        # Check if the engine is valid
        if self.engine is None:
            self.logger.error("Failed to create a valid database engine.")
            return None

        # Query the data using the created engine
        self.df = query_data(self.engine, self.sql_query)

        # Log the success or failure of the data loading
        if self.df is not None:
            self.logger.info("Successfully loaded data.")
        else:
            self.logger.error("Failed to load data.")

        return self.df
    def rename_columns(self):
        # Extract the columns to rename from the configuration
        column1, column2 = list(self.columns_to_rename.keys())[0], list(self.columns_to_rename.values())[0]

        # Temporarily rename one of the columns to avoid a naming conflict
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"

        self.logger.info(f"Swapped columns: {column1} with {column2}")

        # Perform the swap
        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})
    
    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        self.df[abs_column] = self.df[abs_column].abs()
        self.df[column_name] = self.df[column_name].apply(lambda crop: self.values_to_rename.get(crop, crop))

    def weather_station_mapping(self):
        weather_mapping_df = read_from_web_CSV(self.weather_map_data)
        self.df = pd.merge(self.df, weather_mapping_df, on='Field_ID', how='left')
        return self.df
    
    def process(self):
        self.ingest_sql_data()
       # Step 2: Rename columns
        self.rename_columns()

        # Step 3: Apply corrections
        self.apply_corrections()

        # Step 4: Weather station mapping
        self.weather_station_mapping()

        # You can add more processing steps as needed

        # Log completion of the processing
        self.logger.info("Data processing completed.")
    
