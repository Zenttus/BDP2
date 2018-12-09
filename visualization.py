from bokeh.io import output_file, show
from bokeh.layouts import gridplot
from bokeh.plotting import figure
import random


def load_data(folder, limit=24):
    # Hash tags
    f = open(folder+'/hashtags', 'r')
    hash_tags = f.readlines()[-1].split(', ')
    f.close()

    # Words
    f = open(folder + '/words', 'r')
    words = f.readlines()[-1].split(', ')
    f.close()

    # Users
    f = open(folder + '/users', 'r')
    users = f.readlines()[-1].split(', ')
    f.close()

    # Key Words
    key_words = []
    f = open(folder + '/keywords', 'r')
    lines = f.readlines()
    count = 0
    for line in reversed(lines):
        if count > limit: # Number of data points for graph
            break
        count += 1
        key_words.append(line.split(', '))
    f.close()

    return hash_tags, words, users, key_words


def run(visuals=True):
    r = lambda: random.randint(0, 255)

    print("Loading data...")
    hashtags, words, users, keywords = load_data('./data')

    print("Creating visuals...")
    output_file('results.html', title="Twitter data")

    # Trending HashTags
    s1 = figure(x_range=hashtags[0].split(' '), width=450, plot_height=250, title='Top HashTags (Last Update:' + str(hashtags[-1]) + ')', toolbar_location=None, tools="")
    s1.yaxis.axis_label = 'Count'
    s1.vbar(x=hashtags[0].split(' '), top=hashtags[1].split(' '), width=0.9, color='#%02X%02X%02X' % (r(), r(), r()))

    # Trending Words
    s2 = figure(x_range=words[0].split(' '), width=450, height=250, title='Trending Words (Last Update:' + str(words[-1]) + ')', toolbar_location=None, tools="")
    s2.yaxis.axis_label = 'Count'
    s2.vbar(x=words[0].split(' '), top=words[1].split(' '), width=0.9, color='#%02X%02X%02X' % (r(), r(), r()))

    # Top Participants
    s3 = figure(x_range=users[0].split(' '), width=450, height=250, title='Top Users (Last Update:' + str(users[-1]) + ')', toolbar_location=None, tools="")
    s3.yaxis.axis_label = 'Count'
    s3.vbar(x=users[0].split(' '), top=users[1].split(' '), width=0.9, color='#%02X%02X%02X' % (r(), r(), r()))

    # Keywords
    hours = [int(line[-1].rstrip()) for line in keywords]
    d = [line[1].split(' ') for line in keywords]

    s4 = figure(width=1000, height=500, title='Keywords', toolbar_location=None, tools="")
    for w in range(len(d[0])):
        s4.line(hours, [int(point[w].rstrip()) for point in d], color='#%02X%02X%02X' % (r(), r(), r()), legend=keywords[0][0].split(' ')[w])
    s4.legend.location = 'top_right'
    s4.xaxis.axis_label = 'Time (hrs)'
    s4.yaxis.axis_label = 'Count'
    # put all the plots in a grid layout
    p = gridplot([[s1, s2, s3], [s4]])

    # show the results
    print('Done')
    if visuals:
        show(p)


run()