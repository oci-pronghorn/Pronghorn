(function( $ ) {
	$.fn.minimap = function( $mapSource ) {
		var x, y, l, t, w, h;
		var $window = $( window );
		var $minimap = this;
		var minimapWidth = $minimap.width();
		var minimapHeight = $minimap.height();
		var $viewport = $( "<div></div>" ).addClass( "minimap-viewport" );
		$minimap.append( $viewport );
		synchronize();

		$window.on( "resize", synchronize );
		$mapSource.on( "scroll", synchronize );
		$mapSource.on( "drag", init );
		$minimap.on( "mousedown touchstart", down );

		function down( e ) {
			var moveEvent, upEvent;
			var pos = $minimap.position();
			x = Math.round( pos.left + l + w / 2 );
			y = Math.round( pos.top + t + h / 2) ;
			move( e );

			if ( e.type === "touchstart" ) {
				moveEvent = "touchmove.minimapDown";
				upEvent = "touchend";
			} else {
				moveEvent = "mousemove.minimapDown";
				upEvent = "mouseup";
			}
			$window.on( moveEvent, move );
			$window.one( upEvent, up );
		}

		function move( e ) {
			e.preventDefault();

			if ( e.type.match( /touch/ ) ) {
				if ( e.touches.length > 1 ) {
					return;
				}
				var event = e.touches[ 0 ];
			} else {
				var event = e;
			}

			var dx = event.clientX - x;
			var dy = event.clientY - y;
			if ( l + dx < 0 ) {
				dx = -l;
			}
			if ( t + dy < 0 ) {
				dy = -t;
			}
			if ( l + w + dx > minimapWidth ) {
				dx = minimapWidth - l - w;
			}
			if ( t + h + dy > minimapHeight ) {
				dy = minimapHeight - t - h;
			}

			x += dx;
			y += dy;

			l += dx;
			t += dy;

			var coefX = minimapWidth / $mapSource[ 0 ].scrollWidth;
			var coefY = minimapHeight / $mapSource[ 0 ].scrollHeight;
			var left = l / coefX;
			var top = t / coefY;

			$mapSource[ 0 ].scrollLeft = Math.round( left );
			$mapSource[ 0 ].scrollTop = Math.round( top );

			redraw();
		}

		function up() {
			$window.off( ".minimapDown" );
		}

		function synchronize() {
			var dims = [ $mapSource.width(), $mapSource.height() ];
			var scroll = [ $mapSource.scrollLeft(), $mapSource.scrollTop() ];
			var scaleX = minimapWidth / $mapSource[ 0 ].scrollWidth;
			var scaleY = minimapHeight / $mapSource[ 0 ].scrollHeight;

			var lW = dims[ 0 ] * scaleX;
			var lH = dims[ 1 ] * scaleY;
			var lX = scroll[ 0 ] * scaleX;
			var lY = scroll[ 1 ] * scaleY;

			w = Math.round( lW );
			h = Math.round( lH );
			l = Math.round( lX );
			t = Math.round( lY );
			//set the mini viewport dimesions
			redraw();
		}

		function redraw() {
			$viewport.css( {
				width : w,
				height : h,
				left : l,
				top : t
			} );
		}

		function init() {
			$minimap.find( ".minimap-node" ).remove();
			//creating mini version of the supplied children
		}

		init();

		return this;
	}
})( jQuery );
