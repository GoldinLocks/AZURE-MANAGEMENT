from configparser import ConfigParser

# Initialize the Parser.
config = ConfigParser()

# Add the Section.
config.add_section('main')

# Set the Values.
config.set('main', 'SERVER_NAME', 'ml-data-server')
config.set('main', 'DATABASE_NAME', 'trading-sql-db')
config.set('main', 'USERNAME', 'srose')
config.set('main', 'PASSWORD', '1v0717U7')

# Write the File.
with open(file='config/config.ini', mode='w+') as f:
    config.write(f)
