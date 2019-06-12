package edu.buffalo.cse.cse486586.groupmessenger2;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import java.lang.reflect.Array;

public class DatabaseHelper extends SQLiteOpenHelper {
    public static final String DATABASE = "GroupMessaging";
    public static final String TABLE = "Message";
    public static final String KEY_COLUMN = "`key`";
    public static final String VALUE_COLUMN = "value";
    public static final String KEY = "key";

    public DatabaseHelper(Context context) {
        super(context, DATABASE, null, 1);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("create table " + TABLE + " (" + KEY_COLUMN + " TEXT," + VALUE_COLUMN + " TEXT)");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("Drop table IF EXISTS " + TABLE);
        onCreate(db);
    }

    public long insertOrUpdateData(ContentValues contentValues){
        SQLiteDatabase db = this.getWritableDatabase();
        String key = contentValues.get(KEY).toString();
        String value = contentValues.get(VALUE_COLUMN).toString();
        String[] arr = {KEY_COLUMN};
        Cursor cursor = db.query(TABLE, arr, KEY_COLUMN + "= '" + key + "'" , null,null,null,null);
        long id = 0;
        if(cursor.getCount() == 1){
            ContentValues newContentValues = new ContentValues();
            newContentValues.put(KEY, key);
            newContentValues.put(VALUE_COLUMN, value);
            db.update(TABLE, newContentValues, KEY_COLUMN + " = ?", new String[]{key});
        }
        else{
            id =  db.insert(TABLE, null, contentValues);
        }
        return id;
    }

    public boolean insertData(String key, String value){
        SQLiteDatabase db = this.getWritableDatabase();
        ContentValues contentValues = new ContentValues();
        contentValues.put(KEY, key);
        contentValues.put(VALUE_COLUMN, value);
        if(db.insert(TABLE, null, contentValues) != -1){
            return true;
        }
        return false;
    }


    public Cursor getValue(String table, String[] columns, String selection,
                           String[] selectionArgs, String groupBy, String having,
                           String orderBy){
        SQLiteDatabase db = this.getWritableDatabase();
        return  db.query(TABLE,columns,KEY_COLUMN + "= '" + selection + "'", selectionArgs,groupBy,having,orderBy);
    }
}
