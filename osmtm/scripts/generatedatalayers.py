# -*- coding: utf-8 -*-

import transaction

from sqlalchemy import engine_from_config, func, select
from sqlalchemy.orm.exc import NoResultFound


from pyramid.paster import (
    get_appsettings,
    setup_logging,
)

from ..models import (
    DBSession,
    Project,
    Dataset,
    TaskData,
    Task,
    TaskState,
    License,
    Base,
)

from ..utils import load_local_settings, compute_checksum


from sqlalchemy.orm import configure_mappers
from sqlalchemy_i18n.manager import translation_manager

import sys
import geojson
import shapely.wkt
import shapely.geometry
import io


def main():
    project_id, dataset_fn = sys.argv[1:]

    setup_logging('development.ini')
    settings = get_appsettings('development.ini')

    load_local_settings(settings)

    engine = engine_from_config(settings, 'sqlalchemy.')
    conn = engine.connect()
    DBSession.configure(bind=engine)

    translation_manager.options.update({
        'locales': settings['available_languages'].split(),
        'get_locale_fallback': True
    })
    configure_mappers()

    with io.open(dataset_fn, 'r', encoding='utf8') as fh:
        geojson_data = fh.read()
        cksum = compute_checksum(geojson_data)

    with transaction.manager:
        try:
            dataset = DBSession.query(Dataset).filter_by(
                project_id=project_id).one()
            if dataset.checksum == cksum:
                print "Dataset unchanged.  Quit now."
                return 0

            dataset.data = geojson_data
            dataset.checksum = cksum
        except NoResultFound:
            dataset = Dataset(name='test', project_id=project_id,
                              data=geojson_data, license_id=1, checksum=cksum)

        DBSession.add(dataset)

        datalayers = {}
        tasks = DBSession.query(Task).filter_by(project_id=project_id).all()
        for task in tasks:
            datalayers[task.id] = []

        for feature in geojson.loads(geojson_data).features:

            # RFC7946 specifies only WGS84 for CSR.
            q = select([Task.id],
                       func.ST_Contains(Task.geometry, 'SRID=4326;' + shapely.wkt.dumps(shapely.geometry.shape(feature.geometry))))
            for row in conn.execute(q).fetchall():
                datalayers[row[0]].append(feature)

        for task_id in datalayers:
            datalayer_geojson = geojson.FeatureCollection(datalayers[task_id])
            cksum = compute_checksum(geojson.dumps(datalayer_geojson))
            task = DBSession.query(Task).filter_by(
                project_id=project_id, id=task_id).first()

            try:
                datalayer = DBSession.query(TaskData).filter_by(
                    project_id=project_id, task_id=task_id).one()
                if datalayer.checksum == cksum:
                    continue
                else:
                    datalayer.data = geojson.dumps(datalayer_geojson)
                    datalayer.checksum = cksum
            except NoResultFound:
                datalayer = TaskData(project_id=project_id, dataset_id=dataset.id,
                                      task_id=task_id, data=geojson.dumps(datalayer_geojson), checksum=cksum)

            if len(datalayers[task_id]) == 0:
                task.states.append(TaskState(state=TaskState.state_validated))
            else:
                task.states.append(TaskState(state=TaskState.state_ready))

            DBSession.add(task)
            DBSession.add(datalayer)
