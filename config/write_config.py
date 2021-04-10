from configparser import ConfigParser

# Initialize the Parser.
config = ConfigParser()

# Add the Section.
config.add_section('main')

# Set the Values.
config.set('main', 'SERVER_NAME', 'SERVER_NAME')
config.set('main', 'DATABASE_NAME', 'DATABASE_NAME')
config.set('main', 'USERNAME', 'USERNAME')
config.set('main', 'PASSWORD', 'PASSWORD')

# Write the File.
with open(file='config/config.ini', mode='w+') as f:
    config.write(f)
