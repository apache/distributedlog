BUNDLER_VERSION = 1.15.3
BUNDLE = bundle _${BUNDLER_VERSION}_
JEKYLL = ${BUNDLE} exec jekyll

setup:
	gem install bundler \
		-v ${BUNDLER_VERSION} \
		--no-rdoc \
		--no-ri
	NOKOGIRI_USE_SYSTEM_LIBRARIES=true ${BUNDLE} install
	wget https://bootstrap.pypa.io/get-pip.py
	chmod +x get-pip.py
	python get-pip.py
	rm get-pip.py
	pip install docutils pygments

local:
	./build.sh local

staging:
	./build.sh staging

production:
	./build.sh production

apache:
	./build.sh apache

serve: 
	./build.sh local /tmp true
