import pickle

favorite_color = { "lion": "yellow", "kitty": "red" }

# marshall/serialize/pickle
pickle.dump(favorite_color, open( "save.p", "wb" ))

# unmarshal/deserialize/unpickle
favorite_color_loaded = pickle.load( open( "save.p", "rb" ) )
