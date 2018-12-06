from bokeh.io import output_file, show
from bokeh.layouts import gridplot
from bokeh.plotting import figure
import random

def load_data(folder, limit=12):
    # Hash tags
    hash_tags = []
    f = open(folder+'/hashtags', 'r')
    lines = f.readlines()
    for line in lines:
        hash_tags.append(line.split(','))
    f.close()

    # Words
    words = []
    f = open(folder + '/words', 'r')
    lines = f.readlines()
    for line in lines:
        words.append(line.split(','))
    f.close()

    # Users
    users = []
    f = open(folder + '/users', 'r')
    lines = f.readlines()
    for line in lines:
        users.append(line.split(','))
    f.close()

    # Key Words
    key_words = []
    f = open(folder + '/keywords', 'r')
    lines = f.readlines()
    count = 0
    for line in reversed(lines):
        if count > limit:
            break
        count += 1
        key_words.append(line.split(','))
    f.close()
    return hash_tags, words, users, key_words


def run(visuals=True):
    r = lambda: random.randint(0, 255)

    print("Loading data...")
    hashtags, words, users, keywords_data = load_data('./data')

    print("Creating visuals...")
    output_file('results.html', title="Twitter data")

    # Trending HashTags
    s1 = figure(x_range=[h[0] for h in hashtags], width=450, plot_height=250, title='Top HashTags (Last Update:' + str(hashtags[-1][-1]) + ')', toolbar_location=None, tools="")
    s1.yaxis.axis_label = 'Count'
    s1.vbar(x=[h[0] for h in hashtags], top=[int(h[1]) for h in hashtags], width=0.9, color='#%02X%02X%02X' % (r(), r(), r()))

    # Trending Words
    s2 = figure(x_range=[w[0] for w in words], width=450, height=250, title='Trending Words (Last Update:' + str(words[-1][-1]) + ')', toolbar_location=None, tools="")
    s2.yaxis.axis_label = 'Count'
    s2.vbar(x=[w[0] for w in words], top=[int(w[1]) for w in words], width=0.9, color='#%02X%02X%02X' % (r(), r(), r()))

    # Top Participants
    s3 = figure(x_range=[u[0] for u in users], width=450, height=250, title='Top Users (Last Update:' + str(users[-1][-1]) + ')', toolbar_location=None, tools="")
    s3.yaxis.axis_label = 'Count'
    s3.vbar(x=[u[0] for u in users], top=[int(u[1]) for u in users], width=0.9, color='#%02X%02X%02X' % (r(), r(), r()))

    # Keywords
    keywords = ['Trump', 'Flu', 'Zika', 'Diarrhea', 'Ebola', 'Headache', 'Measles']
    hours = [int(k[1]) for k in reversed(keywords_data)]
    d = [line[0].split(' ') for line in reversed(keywords_data)]


    s4 = figure(width=1000, height=500, title='Keywords', toolbar_location=None, tools="")
    for w in range(len(keywords)):
        s4.line(hours, [int(n[w]) for n in d], color='#%02X%02X%02X' % (r(), r(), r()), legend=keywords[w])
    s4.legend.location = 'top_right'
    s4.xaxis.axis_label = 'Time (hrs)'
    s4.yaxis.axis_label = 'Count'

    # put all the plots in a grid layout
    p = gridplot([[s1, s2, s3], [s4]])

    # show the results
    print('Done')
    if visuals:
        show(p)
