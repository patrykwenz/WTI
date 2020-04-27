from wtiproj05.rest_api import app
import cherrypy

cherrypy.tree.graft(app.wsgi_app, '/')
cherrypy.config.update({'server.socket_host': '127.0.0.1',
                        'server.socket_port': 9898,
                        'server.thread_pool': 100,
                        'engine.autoreload.on': False,
                        })

if __name__ == '__main__':
    cherrypy.engine.start()
